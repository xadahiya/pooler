def all_users_set() -> str:
    """
    Returns a string representing the set of all users.

    This function returns the string 'allUsers', which represents the set of all users. It can be used to indicate that an operation or action applies to all users in a system or application.

    Returns:
        str: A string representing the set of all users.

    Example:
        >>> all_users_set()
        'allUsers'

    """
    return 'allUsers'


def user_details_htable(email: str) -> str:
    """
    Returns the user details hash table key for the given email.

    Args:
        email (str): The email address of the user.

    Returns:
        str: The hash table key for the user details.

    Example:
        >>> user_details_htable('example@example.com')
        'user:example@example.com'

    """
    return f'user:{email}'


def user_active_api_keys_set(email: str) -> str:
    """
    Sets the active API keys for a user.

    Args:
        email (str): The email address of the user.

    Returns:
        str: A string representing the key used to store the active API keys for the user.
    """
    return f'user:{email}:apikeys'


def user_revoked_api_keys_set(email: str) -> str:
    """
    Sets the key for the revoked API keys of a user with the given email.

    Args:
        email (str): The email of the user.

    Returns:
        str: The key for the revoked API keys of the user.
    """
    return f'user:{email}:revokedApikeys'


def api_key_to_owner_key(api_key: str) -> str:
    """
    Converts an API key to an owner key.

    Args:
        api_key (str): The API key to be converted.

    Returns:
        str: The corresponding owner key.
    """
    return f'apikey:{api_key}:owner'
