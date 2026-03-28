"""
Claude Batch API Wrapper
Handles batch processing of multiple prompts using Anthropic's Message Batches API.
"""

import asyncio
import json
from typing import Dict, Any, List, Optional
from dataclasses import dataclass
from enum import Enum

import httpx
import structlog
from tenacity import retry, stop_after_attempt, wait_exponential

from config import get_settings

logger = structlog.get_logger()


class BatchItemStatus(Enum):
    """Status of a batch item."""
    PENDING = "pending"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    ERRORED = "errored"
    CANCELED = "canceled"


@dataclass
class BatchRequest:
    """A single request in a batch."""
    custom_id: str
    prompt: str
    max_tokens: int = 4096


@dataclass
class BatchResponse:
    """Response from a batch item."""
    custom_id: str
    status: BatchItemStatus
    content: Optional[str] = None
    error: Optional[str] = None


class ClaudeBatchProcessor:
    """
    Processes multiple Claude requests using the Message Batches API.

    The Message Batches API allows submitting up to 10,000 requests in a batch,
    with results available within 24 hours (usually much faster).
    Cost is 50% less than individual API calls.
    """

    API_BASE = "https://api.anthropic.com/v1"

    def __init__(self):
        self.settings = get_settings()
        self._client: Optional[httpx.AsyncClient] = None

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=30.0),
            headers={
                "x-api-key": self.settings.anthropic_api_key,
                "anthropic-version": "2023-06-01",
                "anthropic-beta": "message-batches-2024-09-24",
                "content-type": "application/json",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()

    async def create_batch(self, requests: List[BatchRequest]) -> str:
        """
        Create a new message batch.

        Args:
            requests: List of BatchRequest objects

        Returns:
            Batch ID
        """
        logger.info("Creating Claude batch", request_count=len(requests))

        # Build batch request body
        batch_requests = []
        for req in requests:
            batch_requests.append({
                "custom_id": req.custom_id,
                "params": {
                    "model": self.settings.claude_model,
                    "max_tokens": req.max_tokens,
                    "messages": [
                        {"role": "user", "content": req.prompt}
                    ],
                },
            })

        response = await self._client.post(
            f"{self.API_BASE}/messages/batches",
            json={"requests": batch_requests},
        )

        if response.status_code != 200:
            error = response.text
            logger.error("Failed to create batch", status=response.status_code, error=error)
            raise Exception(f"Failed to create batch: {response.status_code} - {error}")

        data = response.json()
        batch_id = data["id"]

        logger.info("Batch created", batch_id=batch_id)

        return batch_id

    async def get_batch_status(self, batch_id: str) -> Dict[str, Any]:
        """Get the current status of a batch."""
        response = await self._client.get(
            f"{self.API_BASE}/messages/batches/{batch_id}"
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get batch status: {response.status_code}")

        return response.json()

    async def poll_for_completion(self, batch_id: str) -> List[BatchResponse]:
        """
        Poll batch until all items complete.

        Returns:
            List of BatchResponse objects
        """
        logger.info("Polling batch for completion", batch_id=batch_id)

        while True:
            status = await self.get_batch_status(batch_id)

            processing_status = status.get("processing_status")
            request_counts = status.get("request_counts", {})

            logger.info(
                "Batch status",
                batch_id=batch_id,
                status=processing_status,
                succeeded=request_counts.get("succeeded", 0),
                errored=request_counts.get("errored", 0),
                processing=request_counts.get("processing", 0),
            )

            if processing_status == "ended":
                break

            await asyncio.sleep(self.settings.batch_poll_interval_seconds)

        # Get results
        return await self.get_batch_results(batch_id)

    async def get_batch_results(self, batch_id: str) -> List[BatchResponse]:
        """
        Get results for a completed batch.

        Returns:
            List of BatchResponse objects
        """
        logger.info("Fetching batch results", batch_id=batch_id)

        response = await self._client.get(
            f"{self.API_BASE}/messages/batches/{batch_id}/results"
        )

        if response.status_code != 200:
            raise Exception(f"Failed to get batch results: {response.status_code}")

        # Results come as JSONL
        results = []
        for line in response.text.strip().split("\n"):
            if not line:
                continue

            item = json.loads(line)
            custom_id = item.get("custom_id")

            if item.get("result", {}).get("type") == "succeeded":
                message = item["result"]["message"]
                content = ""
                for block in message.get("content", []):
                    if block.get("type") == "text":
                        content += block.get("text", "")

                results.append(BatchResponse(
                    custom_id=custom_id,
                    status=BatchItemStatus.SUCCEEDED,
                    content=content,
                ))
            else:
                error = item.get("result", {}).get("error", {})
                results.append(BatchResponse(
                    custom_id=custom_id,
                    status=BatchItemStatus.ERRORED,
                    error=str(error),
                ))

        logger.info(
            "Batch results fetched",
            batch_id=batch_id,
            succeeded=sum(1 for r in results if r.status == BatchItemStatus.SUCCEEDED),
            errored=sum(1 for r in results if r.status == BatchItemStatus.ERRORED),
        )

        return results

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=2, min=2, max=8),
    )
    async def send_single_request(self, prompt: str, max_tokens: int = 4096) -> str:
        """
        Send a single synchronous request (for retrying failed batch items).

        Args:
            prompt: The prompt text
            max_tokens: Maximum response tokens

        Returns:
            Response content text
        """
        logger.info("Sending single Claude request")

        response = await self._client.post(
            f"{self.API_BASE}/messages",
            json={
                "model": self.settings.claude_model,
                "max_tokens": max_tokens,
                "messages": [{"role": "user", "content": prompt}],
            },
            headers={
                "x-api-key": self.settings.anthropic_api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
        )

        if response.status_code != 200:
            raise Exception(f"Claude request failed: {response.status_code} - {response.text}")

        data = response.json()
        content = ""
        for block in data.get("content", []):
            if block.get("type") == "text":
                content += block.get("text", "")

        return content

    async def process_batch_with_retry(
        self, requests: List[BatchRequest]
    ) -> Dict[str, str]:
        """
        Process a batch with automatic retry for failed items.

        Args:
            requests: List of BatchRequest objects

        Returns:
            Dict mapping custom_id to response content
        """
        # Create and process batch
        batch_id = await self.create_batch(requests)
        results = await self.poll_for_completion(batch_id)

        # Build results dict
        responses = {}
        failed_items = []

        for result in results:
            if result.status == BatchItemStatus.SUCCEEDED:
                responses[result.custom_id] = result.content
            else:
                failed_items.append(result)
                logger.warning(
                    "Batch item failed",
                    custom_id=result.custom_id,
                    error=result.error,
                )

        # Retry failed items synchronously
        if failed_items:
            logger.info("Retrying failed batch items", count=len(failed_items))

            # Build map of custom_id to prompt
            prompt_map = {r.custom_id: r.prompt for r in requests}

            for item in failed_items:
                prompt = prompt_map.get(item.custom_id)
                if prompt:
                    try:
                        content = await self.send_single_request(prompt)
                        responses[item.custom_id] = content
                        logger.info("Retry succeeded", custom_id=item.custom_id)
                    except Exception as e:
                        logger.error(
                            "Retry failed",
                            custom_id=item.custom_id,
                            error=str(e),
                        )
                        responses[item.custom_id] = None

        return responses
