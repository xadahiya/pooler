from pooler.settings.config import settings

failed_query_epochs_redis_q = (
    'failedQueryEpochs:' + settings.namespace +
    ':{}:{}'
)

discarded_query_epochs_redis_q = (
    'discardedQueryEpochs:' + settings.namespace +
    ':{}:{}'
)

failed_commit_epochs_redis_q = (
    'failedCommitEpochs:' + settings.namespace +
    ':{}:{}'
)

cached_block_details_at_height = (
    'uniswap:blockDetail:' + settings.namespace + ':blockDetailZset'
)
project_hits_payload_data_key = 'hitsPayloadData'
powerloom_broadcast_id_zset = (
    'powerloom:broadcastID:' + settings.namespace + ':broadcastProcessingStatus'
)
epoch_detector_last_processed_epoch = 'SystemEpochDetector:lastProcessedEpoch'

event_detector_last_processed_block = 'SystemEventDetector:lastProcessedBlock'

projects_dag_verifier_status = (
    'projects:' + settings.namespace + ':dagVerificationStatus'
)

uniswap_eth_usd_price_zset = (
    'uniswap:ethBlockHeightPrice:' + settings.namespace + ':ethPriceZset'
)

rpc_json_rpc_calls = (
    'rpc:jsonRpc:' + settings.namespace + ':calls'
)

rpc_get_event_logs_calls = (
    'rpc:eventLogsCount:' + settings.namespace + ':calls'
)

rpc_web3_calls = (
    'rpc:web3:' + settings.namespace + ':calls'
)

rpc_blocknumber_calls = (
    'rpc:blocknumber:' + settings.namespace + ':calls'
)


# project finalzed data zset
def project_finalized_data_zset(project_id):
    """
    Returns the key for the sorted set that stores the finalized data of a project.

    Args:
        project_id (str): The ID of the project.

    Returns:
        str: The key for the sorted set that stores the finalized data of the project.

    Example:
        >>> project_finalized_data_zset('123')
        'projectID:123:finalizedData'

    """
    return f'projectID:{project_id}:finalizedData'

    # project first epoch hashmap


def project_first_epoch_hmap():
    """
    Returns the first epoch of the project as a heat map.

    Returns:
        str: The first epoch of the project as a heat map, represented as a string.

    """
    return 'projectFirstEpoch'


def source_chain_id_key():
    """
    Returns the key for the source chain ID.

    Returns:
        str: The key for the source chain ID.

    """
    return 'sourceChainId'


def source_chain_block_time_key():
    """
    Returns the key for the source chain block time.

    Returns:
        str: The key for the source chain block time.
    """
    return 'sourceChainBlockTime'


def source_chain_epoch_size_key():
    """
    Returns the key for the source chain epoch size. This key is used to access the source chain epoch size value in a dictionary or any other data structure. The source chain epoch size represents the size or length of the source chain epoch.
    """
    return 'sourceChainEpochSize'


def project_last_finalized_epoch_key(project_id):
    """
    Returns the last finalized epoch key for a given project.

    Args:
        project_id (str): The ID of the project.

    Returns:
        str: The last finalized epoch key in the format 'projectID:{project_id}:lastFinalizedEpoch'.
    """
    return f'projectID:{project_id}:lastFinalizedEpoch'


def project_successful_snapshot_submissions_suffix():
    """
    Returns the suffix used to identify the total count of successful snapshot submissions in a project.

    This function returns a string that can be appended to the name of a project to identify the total count of successful snapshot submissions for that project. The suffix is used to provide a standardized naming convention for accessing this information.

    Returns:
        str: The suffix used to identify the total count of successful snapshot submissions in a project.

    Example:
        >>> project_name = 'my_project'
        >>> suffix = project_successful_snapshot_submissions_suffix()
        >>> total_successful_count = project_name + '_' + suffix
        >>> print(total_successful_count)
        my_project_totalSuccessfulSnapshotCount

    """
    return 'totalSuccessfulSnapshotCount'


def project_incorrect_snapshot_submissions_suffix():
    """
    Returns the suffix used for counting the total number of incorrect snapshot submissions in a project.

    Returns:
        str: The suffix used for counting the total number of incorrect snapshot submissions in a project.

    """
    return 'totalIncorrectSnapshotCount'


def project_missed_snapshot_submissions_suffix():
    """
    Returns the suffix used for the key that represents the total count of missed snapshot submissions in a project.

    Returns:
        str: The suffix used for the key representing the total count of missed snapshot submissions.
    """
    return 'totalMissedSnapshotCount'


def project_snapshotter_status_report_key(project_id):
    """
    Returns the key for the status report of a project snapshotter.

    Args:
        project_id (str): The ID of the project.

    Returns:
        str: The key for the status report of the project snapshotter.

    Example:
        >>> project_snapshotter_status_report_key('12345')
        'projectID:12345:snapshotterStatusReport'

    """
    return f'projectID:{project_id}:snapshotterStatusReport'


def stored_projects_key():
    """
    Returns the key used to store project IDs in the database.
    """
    return 'storedProjectIds'
