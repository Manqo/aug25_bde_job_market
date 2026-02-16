from unittest.mock import Mock, patch

from requests import exceptions

from orchestrator.orchestrator import trigger_etl


def test_trigger_etl():
    with patch("orchestrator.orchestrator.requests.post") as mock_post:
        # success
        mock_post.return_value = Mock(status_code=200)
        trigger_etl()
        assert mock_post.call_count == 1

        # already running
        mock_post.reset_mock()
        mock_post.return_value = Mock(status_code=409)
        trigger_etl()
        assert mock_post.call_count == 1

        # failure status
        mock_post.reset_mock()
        mock_post.return_value = Mock(status_code=500, text="Internal Server Error")
        trigger_etl()
        assert mock_post.call_count == 1

        # exception
        mock_post.reset_mock()
        mock_post.side_effect = exceptions.Timeout()
        trigger_etl()
        assert mock_post.call_count == 1
