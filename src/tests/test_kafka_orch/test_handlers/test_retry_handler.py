
import pytest
from unittest.mock import Mock, AsyncMock


class TestRetryHandler:
    @pytest.fixture
    def retry_handler(self):
        from betflow.kafka_orch.handlers import RetryHandler
        return RetryHandler(max_retries=3, initial_backoff=1.0)

    def test_retry_sync_eventual_success(self, retry_handler):
        """Test sync operation that succeeds after failures."""
        # Create mock with proper name attribute
        mock_func = Mock()
        mock_func.name = 'mock_function'  # Set name directly
        mock_func.side_effect = [
            Exception("First failure"),
            Exception("Second failure"),
            "success"
        ]

        sleep_calls = []
        retry_handler.sleep = lambda x: sleep_calls.append(x)

        result = retry_handler.retry_sync(mock_func)

        assert result == "success"
        assert mock_func.call_count == 3
        assert sleep_calls == [1.0, 2.0]

    def test_retry_sync_max_retries_exceeded(self, retry_handler):
        """Test sync operation that always fails."""
        # Create mock with proper name attribute
        mock_func = Mock()
        mock_func.name = 'mock_function'  # Set name directly
        mock_func.side_effect = Exception("Always fails")

        sleep_calls = []
        retry_handler.sleep = lambda x: sleep_calls.append(x)

        with pytest.raises(Exception) as exc_info:
            retry_handler.retry_sync(mock_func)

        error_message = str(exc_info.value)
        assert "mock_function failed after 3 attempts" in error_message
        assert "Always fails" in error_message
        assert sleep_calls == [1.0, 2.0]
        assert mock_func.call_count == 3

    @pytest.mark.asyncio
    async def test_retry_async_eventual_success(self, retry_handler):
        """Test async operation that succeeds after failures."""
        # Create mock with proper name attribute
        mock_func = AsyncMock()
        mock_func.name = 'mock_function'  # Set name directly
        mock_func.side_effect = [
            Exception("First failure"),
            Exception("Second failure"),
            "success"
        ]

        sleep_calls = []
        retry_handler.async_sleep = AsyncMock(side_effect=lambda x: sleep_calls.append(x))

        result = await retry_handler.retry_async(mock_func)

        assert result == "success"
        assert mock_func.call_count == 3
        assert sleep_calls == [1.0, 2.0]

    @pytest.mark.asyncio
    async def test_retry_async_max_retries_exceeded(self, retry_handler):
        """Test async operation that always fails."""
        # Create mock with proper name attribute
        mock_func = AsyncMock()
        mock_func.name = 'mock_function'  # Set name directly
        mock_func.side_effect = Exception("Always fails")

        sleep_calls = []
        retry_handler.async_sleep = AsyncMock(side_effect=lambda x: sleep_calls.append(x))

        with pytest.raises(Exception) as exc_info:
            await retry_handler.retry_async(mock_func)

        error_message = str(exc_info.value)
        assert "mock_function failed after 3 attempts" in error_message
        assert "Always fails" in error_message
        assert sleep_calls == [1.0, 2.0]
        assert mock_func.call_count == 3

    def test_calculate_delay(self, retry_handler):
        """Test exponential backoff calculation."""
        assert retry_handler._calculate_delay(1) == 1.0
        assert retry_handler._calculate_delay(2) == 2.0
        assert retry_handler._calculate_delay(3) == 4.0