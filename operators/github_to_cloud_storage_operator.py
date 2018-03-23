from tempfile import NamedTemporaryFile
from flatten_json import flatten
import logging
import json

from airflow.utils.decorators import apply_defaults
from airflow.models import BaseOperator
from airflow.hooks import S3Hook, GoogleCloudStorageHook

from github_plugin.hooks.github_hook import GithubHook


class GithubToCloudStorageOperator(BaseOperator):
    """
    Github To Cloud Storage Operator
    :param github_conn_id:           The Github connection id.
    :type github_conn_id:            string
    :param github_org:               The Github organization.
    :type github_org:                string
    :param github_repo:              The Github repository. Required for
                                     commits, commit_comments, issue_comments,
                                     and issues objects.
    :type github_repo:               string
    :param github_object:            The desired Github object. The currently
                                     supported values are:
                                        - commits
                                        - commit_comments
                                        - issue_comments
                                        - issues
                                        - members
                                        - organizations
                                        - pull_requests
                                        - repositories
    :type github_object:             string
    :param payload:                  The associated github parameters to
                                     pass into the object request as
                                     keyword arguments.
    :type payload:                   dict
    :param destination:              The final destination where the data
                                     should be stored. Possible values include:
                                        - GCS
                                        - S3
    :type destination:               string
    :param dest_conn_id:             The destination connection id.
    :type dest_conn_id:              string
    :param bucket:                   The bucket to be used to store the data.
    :type bucket:                    string
    :param key:                      The filename to be used to store the data.
    :type key:                       string
    """

    template_field = ('key',)

    @apply_defaults
    def __init__(self,
                 github_conn_id,
                 github_org,
                 github_object,
                 dest_conn_id,
                 bucket,
                 key,
                 destination='s3',
                 github_repo=None,
                 payload={},
                 **kwargs):
        super().__init__(**kwargs)
        self.github_conn_id = github_conn_id
        self.github_org = github_org
        self.github_repo = github_repo
        self.github_object = github_object
        self.payload = payload
        self.destination = destination
        self.dest_conn_id = dest_conn_id
        self.bucket = bucket
        self.key = key

        if self.github_object.lower() not in ('commits',
                                              'commit_comments',
                                              'issue_comments',
                                              'issues',
                                              'members',
                                              'organizations',
                                              'pull_requests',
                                              'repositories'):
            raise Exception('Specified Github object not currently supported.')

    def execute(self, context):
        g = GithubHook(self.github_conn_id)
        output = []

        if self.github_object not in ('members',
                                      'organizations',
                                      'repositories'):
            if self.github_repo == 'all':
                repos = [repo['name'] for repo in
                         self.paginate_data(g,
                                            self.methodMapper('repositories'))]
                for repo in repos:
                    output.extend(self.retrieve_data(g, repo=repo))
            elif isinstance(self.github_repo, list):
                repos = self.github_repo
                for repo in repos:
                    output.extend(self.retrieve_data(g, repo=repo))
            else:
                output = self.retrieve_data(g, repo=self.github_repo)
        else:
            output = self.retrieve_data(g, repo=self.github_repo)

        self.output_manager(output)

    def output_manager(self, output):
        output = '\n'.join([json.dumps(flatten(record)) for record in output])

        if self.destination.lower() == 's3':
            s3 = S3Hook(self.dest_conn_id)

            s3.load_string(
                string_data=output,
                key=self.key,
                bucket_name=self.bucket,
                replace=True
            )

            s3.connection.close()

        elif self.destination.lower() == 'gcs':
            with NamedTemporaryFile('w') as tmp:
                tmp.write(output)

                gcs = GoogleCloudStorageHook(self.dest_conn_id)

                gcs.upload(
                    bucket=self.bucket,
                    object=self.key,
                    filename=tmp.name,
                )

    def retrieve_data(self, g, repo=None):
        """
        This method builds the endpoint and passes it into
        the "paginate_data" method. It is wrapped in a
        "try/except" in the event that an HTTP Error is thrown.
        This can happen when making a request to a page that
        does not yet exist (e.g. requesting commits from a
        repo that has no commits.)
        """
        try:
            endpoint = self.methodMapper(self.github_object,
                                         self.github_org,
                                         repo)
            return self.paginate_data(g, endpoint)
        except:
            logging.info('Resource is unavailable.')
            return ''

    def paginate_data(self, g, endpoint):
        """
        This method takes care of request building and pagination.
        It retrieves 100 at a time and continues to make
        subsequent requests until it retrieves less than 100 records.
        """
        output = []
        final_payload = {'per_page': 100, 'page': 1}
        for param in self.payload:
            final_payload[param] = self.payload[param]

        response = g.run(endpoint, final_payload).json()
        output.extend(response)
        logging.info('Retrieved: ' + str(final_payload['per_page'] *
                                         final_payload['page']))
        while len(response) == 100:
            final_payload['page'] += 1
            response = g.run(endpoint, final_payload).json()
            logging.info('Retrieved: ' + str(final_payload['per_page'] *
                                             final_payload['page']))
            output.extend(response)
        output = [self.filterMapper(record) for record in output]
        return output

    def methodMapper(self, github_object, org=None, repo=None):
        """
        This method maps the desired object to the relevant endpoint
        according to v3 of the Github API.
        """
        mapping = {"commits": "repos/{0}/{1}/commits".format(org, repo),
                   "commit_comments": "repos/{0}/{1}/comments".format(org, repo),
                   "issue_comments": "repos/{0}/{1}/issues/comments".format(org, repo),
                   "issues": "repos/{0}/{1}/issues".format(org, repo),
                   "members": "orgs/{0}/members".format(org),
                   "organizations": "user/organizations",
                   "pull_requests": "repos/{0}/{1}/pulls".format(org, repo),
                   "repositories": "orgs/{0}/repos".format(self.github_org)
                   }

        return mapping[github_object]

    def filterMapper(self, record):
        """
        This process strips out unnecessary objects (i.e. ones
        that are duplicated in other core objects).
        Example: a commit returns all the same user information
        for each commit as already returned the members endpoint).
        In most cases, id for these striped objects are kept for
        reference although multiple values can be added to the array.

        Labels is currently returned as an array of dicts.
        When flattened, this can cause an undo amount of
        columns with the naming convention labels_0_name,
        labels_1_name, etc. Until a better data model can be
        determined (possibly putting labels in their own table)
        these fields are striped out entirely.

        In situations where there are no desired retention fields,
        "retained" should be set to "None".
        """
        mapping = [{'name': 'commits',
                    'filtered': 'author',
                    'retained': ['id']
                    },
                   {'name': 'commits',
                    'filtered': 'committer',
                    'retained': ['id']
                    },
                   {'name': 'issues',
                    'filtered': 'labels',
                    'retained': None
                    },
                   {'name': 'issue_comments',
                    'filtered': 'user',
                    'retained': ['id']
                    },
                   {'name': 'commit_comments',
                    'filtered': 'user',
                    'retained': ['id']
                    },
                   {'name': 'repositories',
                    'filtered': 'owner',
                    'retained': ['id']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'assignee',
                    'retained': ['id']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'milestone',
                    'retained': ['id']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'head',
                    'retained': ['label']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'base',
                    'retained': ['label']
                    },
                   {'name': 'pull_requests',
                    'filtered': 'user',
                    'retained': ['id']
                    }
                   ]

        def process(record, mapping):
            """
            This method processes the data according to the above mapping.
            There are a number of checks throughout as the specified filtered
            object and desired retained fields will not always exist in each
            record.
            """

            for entry in mapping:
                # Check to see if the filtered value exists in the record
                if (entry['name'] == self.github_object) and (entry['filtered'] in list(record.keys())):
                    # Check to see if any retained fields are desired.
                    # If not, delete the object.
                    if entry['retained']:
                        for retained_item in entry['retained']:
                            # Check to see the filterable object exists in the
                            # specific record. This is not always the case.
                            # Check to see the retained field exists in the
                            # filterable object.
                            if record[entry['filtered']] is not None and\
                                    retained_item in list(record[entry['filtered']].keys()):
                                    # Bring retained field to top level of
                                    # object with snakecasing.
                                    record["{0}_{1}".format(entry['filtered'],
                                                            retained_item)] = \
                                        record[entry['filtered']][retained_item]
                    if record[entry['filtered']] is not None:
                        del record[entry['filtered']]
            return record

        return process(record, mapping)
