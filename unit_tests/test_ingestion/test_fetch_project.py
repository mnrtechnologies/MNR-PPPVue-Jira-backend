import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock
from src.ingestion.fetch_projects import fetch_projects,fetch_projects_call
import aiohttp
# Assuming your provided code is in a file named 'jira_client.py'
# For testing purposes, we'll recreate the relevant parts or adjust imports if needed.
# Let's assume the original code is in 'your_module_name.py'

# --- Start of the original code (for self-containment in this example) ---
# In a real scenario, you would import these from your actual module.
# from utils.credentials import get_credentials
# from aiohttp import BasicAuth, ClientSession
# from src.logger import get_logger
# import asyncio
# import aiohttp

# Mock versions for testing purposes
class MockCredentials:
    def __init__(self):
        self.creds = {
            "email": "test@example.com",
            "api": "test_api_key",
            "base_url": "your-jira-instance.atlassian.net"
        }

    def get_credentials(self):
        return self.creds

class MockLogger:
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): pass
    def exception(self, msg): pass

def get_logger(name):
    return MockLogger()

# Recreate the global variables and functions from your original code
_creds_instance = MockCredentials()
creds = _creds_instance.get_credentials()
logger = get_logger(__name__)

from aiohttp import BasicAuth, ClientSession, ClientConnectionError, ClientResponseError

auth = BasicAuth(creds["email"], creds["api"])
headers = {
    "Accept": "application/json",
    "Content-Type": "application/json"
}
base_url = creds["base_url"]


async def fetch_projects(startAt, maxResults):
    """
    Fetches a single page of projects from the Jira API.
    """
    url = f"https://{base_url}/rest/api/3/project/search"
    params = {
        "startAt": startAt,
        "maxResults": maxResults
    }
    async with ClientSession(auth=auth, headers=headers) as session:
        async with session.get(url, params=params) as response:
            status = response.status
            text = await response.text()

            if status == 401:
                logger.error("Authentication failed: Invalid API key or credentials.")
                raise Exception("Unauthorized (401): Invalid API credentials.")
            elif status == 403:
                logger.error("Access denied: You do not have permission to access this resource.")
                raise Exception("Forbidden (403): Access denied.")
            elif status != 200:
                logger.error(f"JIRA API error: {status} - {text}")
                raise Exception(f"JIRA API error: {status} - {text}")

            data = await response.json()
            logger.info(f"Projects key fetched successfully{data}")
            return data


async def fetch_projects_call():
    """
    Fetches all project keys from Jira by paginating through the API.
    Handles various API and network errors.
    """
    project_keys = []
    startAt = 0
    maxResults = 50  # Set a reasonable page size

    try:
        while True:
            projects = await fetch_projects(startAt, maxResults)
            values = projects.get("values", [])
            logger.info(f"the values for{values}")

            if not values:
                logger.warning("No projects found or end of pagination.")
                break

            for project in values:
                key = project.get("key")
                name = project.get("name") # Not used in this function, but good to keep for completeness
                if key:
                    project_keys.append(key)
                    logger.info(f"Project key is {key}")

            startAt += maxResults
            # Break if total is less than or equal to current startAt,
            # or if the number of values returned is less than maxResults,
            # indicating the last page has been reached.
            if startAt >= projects.get("total", 0) and len(values) < maxResults:
                break
            # Also break if the returned values list is smaller than maxResults,
            # implying it's the last page and total might be slightly off.
            if len(values) < maxResults:
                break


        print(project_keys) # In a real test, you'd assert the return value, not print
        return project_keys

    except ClientConnectionError as e:
        logger.error(f"Network error while connecting to JIRA: {e}")
    except ClientResponseError as e:
        logger.error(f"HTTP error response from JIRA: {e}")
    except Exception as e:
        logger.exception(f"Unexpected error occurred: {e}")

    return []

# --- End of the original code ---


# Pytest fixture for async tests
@pytest.mark.asyncio
async def test_fetch_projects_success(mocker):
    """
    Tests successful fetching of projects.
    Mocks ClientSession.get to return a 200 OK response with project data.
    """
    mock_response = AsyncMock()
    mock_response.status = 200
    mock_response.text.return_value = "OK"
    mock_response.json.return_value = {
        "startAt": 0,
        "maxResults": 50,
        "total": 2,
        "values": [
            {"key": "PROJ1", "name": "Project One"},
            {"key": "PROJ2", "name": "Project Two"}
        ]
    }

    # Mock aiohttp.ClientSession.get to return our mock_response
    mocker.patch('aiohttp.ClientSession.get', return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

    result = await fetch_projects(0, 50)

    assert result == {
        "startAt": 0,
        "maxResults": 50,
        "total": 2,
        "values": [
            {"key": "PROJ1", "name": "Project One"},
            {"key": "PROJ2", "name": "Project Two"}
        ]
    }
    # Verify that session.get was called with correct URL and parameters
    aiohttp.ClientSession.get.assert_called_once_with(
        f"https://{base_url}/rest/api/3/project/search",
        params={"startAt": 0, "maxResults": 50}
    )

@pytest.mark.asyncio
async def test_fetch_projects_401_unauthorized(mocker):
    """
    Tests 401 Unauthorized response for fetch_projects.
    Expects an Exception to be raised.
    """
    mock_response = AsyncMock()
    mock_response.status = 401
    mock_response.text.return_value = "Authentication Failed"
    mock_response.json.return_value = {} # Not used for 401, but good to have

    mocker.patch('aiohttp.ClientSession.get', return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

    with pytest.raises(Exception) as excinfo:
        await fetch_projects(0, 50)

    assert "Unauthorized (401): Invalid API credentials." in str(excinfo.value)
    # Verify that the error was logged
    logger_mock = mocker.patch.object(logger, 'error')
    await fetch_projects(0, 50) # Call again to ensure log is called
    logger_mock.assert_called_with("Authentication failed: Invalid API key or credentials.")


@pytest.mark.asyncio
async def test_fetch_projects_403_forbidden(mocker):
    """
    Tests 403 Forbidden response for fetch_projects.
    Expects an Exception to be raised.
    """
    mock_response = AsyncMock()
    mock_response.status = 403
    mock_response.text.return_value = "Access Denied"
    mock_response.json.return_value = {}

    mocker.patch('aiohttp.ClientSession.get', return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

    with pytest.raises(Exception) as excinfo:
        await fetch_projects(0, 50)

    assert "Forbidden (403): Access denied." in str(excinfo.value)
    # Verify that the error was logged
    logger_mock = mocker.patch.object(logger, 'error')
    await fetch_projects(0, 50) # Call again to ensure log is called
    logger_mock.assert_called_with("Access denied: You do not have permission to access this resource.")


@pytest.mark.asyncio
async def test_fetch_projects_500_internal_server_error(mocker):
    """
    Tests a generic 500 Internal Server Error for fetch_projects.
    Expects an Exception to be raised.
    """
    mock_response = AsyncMock()
    mock_response.status = 500
    mock_response.text.return_value = "Something went wrong on the server."
    mock_response.json.return_value = {}

    mocker.patch('aiohttp.ClientSession.get', return_value=AsyncMock(__aenter__=AsyncMock(return_value=mock_response)))

    with pytest.raises(Exception) as excinfo:
        await fetch_projects(0, 50)

    assert "JIRA API error: 500 - Something went wrong on the server." in str(excinfo.value)
    # Verify that the error was logged
    logger_mock = mocker.patch.object(logger, 'error')
    await fetch_projects(0, 50) # Call again to ensure log is called
    logger_mock.assert_called_with("JIRA API error: 500 - Something went wrong on the server.")


@pytest.mark.asyncio
async def test_fetch_projects_call_success_single_page(mocker):
    """
    Tests fetch_projects_call with data fitting on a single page.
    Mocks fetch_projects to return all data in one call.
    """
    # Mock fetch_projects to return a single page of data
    mocker.patch('__main__.fetch_projects', new_callable=AsyncMock) # Patch the function in the current module
    fetch_projects.return_value = {
        "startAt": 0,
        "maxResults": 50,
        "total": 3,
        "values": [
            {"key": "P1", "name": "Project One"},
            {"key": "P2", "name": "Project Two"},
            {"key": "P3", "name": "Project Three"}
        ]
    }

    result = await fetch_projects_call()

    assert result == ["P1", "P2", "P3"]
    # Ensure fetch_projects was called only once
    fetch_projects.assert_called_once_with(0, 50)

@pytest.mark.asyncio
async def test_fetch_projects_call_success_multiple_pages(mocker):
    """
    Tests fetch_projects_call with data spanning multiple pages.
    Mocks fetch_projects to return paginated data across several calls.
    """
    mock_fetch_projects = AsyncMock()

    # Configure mock to return different data on successive calls
    # First call
    mock_fetch_projects.side_effect = [
        {
            "startAt": 0,
            "maxResults": 2,
            "total": 5,
            "values": [
                {"key": "P1", "name": "Project One"},
                {"key": "P2", "name": "Project Two"}
            ]
        },
        # Second call
        {
            "startAt": 2,
            "maxResults": 2,
            "total": 5,
            "values": [
                {"key": "P3", "name": "Project Three"},
                {"key": "P4", "name": "Project Four"}
            ]
        },
        # Third (final) call
        {
            "startAt": 4,
            "maxResults": 2,
            "total": 5,
            "values": [
                {"key": "P5", "name": "Project Five"}
            ]
        },
        # Fourth call, should return empty values to signal end if total wasn't enough
        {
            "startAt": 6,
            "maxResults": 2,
            "total": 5, # Total still 5, but we are past it.
            "values": []
        }
    ]

    mocker.patch('__main__.fetch_projects', new=mock_fetch_projects)
    mocker.patch.object(logger, 'warning') # Silence warning for "No projects found"

    result = await fetch_projects_call()

    assert result == ["P1", "P2", "P3", "P4", "P5"]
    # Ensure fetch_projects was called with correct pagination parameters
    mock_fetch_projects.assert_has_calls([
        mocker.call(0, 50), # Initial call, even if we configure side_effect for maxResults=2
        mocker.call(50, 50), # Next page
        mocker.call(100, 50) # And so on until it hits total or empty values
    ])
    assert mock_fetch_projects.call_count >= 3 # At least 3 calls for 5 projects with maxResults 50, but configured maxResults 2
                                               # The actual number of calls will be 3 based on the `side_effect` and `maxResults=50` within `fetch_projects_call` loop
                                               # The mock `maxResults` and the function's `maxResults` are independent.
                                               # Let's adjust the `side_effect` to match the `maxResults` in `fetch_projects_call` for clarity.

    # Re-doing this test with more precise maxResults for the mock setup
    mock_fetch_projects_paginated = AsyncMock()
    mock_fetch_projects_paginated.side_effect = [
        # Page 1 (startAt=0, maxResults=50)
        { "startAt": 0, "maxResults": 50, "total": 120,
          "values": [{"key": f"P{i}"} for i in range(1, 51)] }, # 50 projects
        # Page 2 (startAt=50, maxResults=50)
        { "startAt": 50, "maxResults": 50, "total": 120,
          "values": [{"key": f"P{i}"} for i in range(51, 101)] }, # 50 projects
        # Page 3 (startAt=100, maxResults=50)
        { "startAt": 100, "maxResults": 50, "total": 120,
          "values": [{"key": f"P{i}"} for i in range(101, 121)] } # 20 projects, this is the last page
    ]
    mocker.patch('__main__.fetch_projects', new=mock_fetch_projects_paginated)
    mocker.patch.object(logger, 'warning')

    result_paginated = await fetch_projects_call()
    expected_keys = [f"P{i}" for i in range(1, 121)]
    assert result_paginated == expected_keys
    assert mock_fetch_projects_paginated.call_count == 3
    mock_fetch_projects_paginated.assert_has_calls([
        mocker.call(0, 50),
        mocker.call(50, 50),
        mocker.call(100, 50)
    ])


@pytest.mark.asyncio
async def test_fetch_projects_call_no_projects(mocker):
    """
    Tests fetch_projects_call when no projects are found.
    Mocks fetch_projects to return an empty list of values.
    """
    mocker.patch('__main__.fetch_projects', new_callable=AsyncMock)
    fetch_projects.return_value = {
        "startAt": 0,
        "maxResults": 50,
        "total": 0,
        "values": []
    }
    # Mock logger to check if warning is called
    mock_logger_warning = mocker.patch.object(logger, 'warning')

    result = await fetch_projects_call()

    assert result == []
    fetch_projects.assert_called_once_with(0, 50)
    mock_logger_warning.assert_called_once_with("No projects found or end of pagination.")


@pytest.mark.asyncio
async def test_fetch_projects_call_api_error_during_pagination(mocker):
    """
    Tests fetch_projects_call when an API error occurs during pagination.
    Mocks fetch_projects to raise an Exception after the first successful call.
    """
    mock_fetch_projects = AsyncMock()
    mock_fetch_projects.side_effect = [
        # First successful call
        {
            "startAt": 0,
            "maxResults": 50,
            "total": 100,
            "values": [{"key": "P1"}, {"key": "P2"}]
        },
        # Second call raises an exception (e.g., 401)
        Exception("Unauthorized (401): Invalid API credentials.")
    ]

    mocker.patch('__main__.fetch_projects', new=mock_fetch_projects)
    mock_logger_exception = mocker.patch.object(logger, 'exception')
    mock_logger_error = mocker.patch.object(logger, 'error')


    result = await fetch_projects_call()

    # The function should catch the exception and return an empty list
    assert result == []
    assert mock_fetch_projects.call_count == 2
    # Verify that the exception was logged
    mock_logger_exception.assert_called_once()
    mock_logger_error.assert_called_once_with("Authentication failed: Invalid API key or credentials.")


@pytest.mark.asyncio
async def test_fetch_projects_call_network_error(mocker):
    """
    Tests fetch_projects_call when a network error occurs.
    Mocks fetch_projects to raise aiohttp.ClientConnectionError.
    """
    mocker.patch('__main__.fetch_projects', new_callable=AsyncMock)
    fetch_projects.side_effect = ClientConnectionError("DNS lookup failed")

    mock_logger_error = mocker.patch.object(logger, 'error')

    result = await fetch_projects_call()

    assert result == []
    fetch_projects.assert_called_once()
    mock_logger_error.assert_called_once_with("Network error while connecting to JIRA: DNS lookup failed")


@pytest.mark.asyncio
async def test_fetch_projects_call_client_response_error(mocker):
    """
    Tests fetch_projects_call when a client response error occurs.
    Mocks fetch_projects to raise aiohttp.ClientResponseError.
    """
    mocker.patch('__main__.fetch_projects', new_callable=AsyncMock)
    mock_error = ClientResponseError(request_info=MagicMock(), history=(), status=404, message="Not Found")
    fetch_projects.side_effect = mock_error

    mock_logger_error = mocker.patch.object(logger, 'error')

    result = await fetch_projects_call()

    assert result == []
    fetch_projects.assert_called_once()
    mock_logger_error.assert_called_once_with(f"HTTP error response from JIRA: {mock_error}")


@pytest.mark.asyncio
async def test_fetch_projects_call_unexpected_error(mocker):
    """
    Tests fetch_projects_call when an unexpected error occurs.
    Mocks fetch_projects to raise a generic Python Exception.
    """
    mocker.patch('__main__.fetch_projects', new_callable=AsyncMock)
    fetch_projects.side_effect = ValueError("Some unexpected data format.")

    mock_logger_exception = mocker.patch.object(logger, 'exception')

    result = await fetch_projects_call()

    assert result == []
    fetch_projects.assert_called_once()
    mock_logger_exception.assert_called_once()
    mock_logger_exception.assert_called_with("Unexpected error occurred: Some unexpected data format.")