"""Storage infrastructure implementation for CSV file writing."""

import csv
import logging
from pathlib import Path

import aiofiles

logger = logging.getLogger(__name__)


class CSVStorageWriter:
    """CSV file writer using async I/O."""

    def __init__(self, output_dir: str = "/data"):
        """
        Initialize CSV storage writer.

        Args:
            output_dir: Directory path for output files
        """
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    async def write_csv(self, data: list[dict], filename: str) -> Path:
        """
        Write data to CSV file asynchronously.

        Args:
            data: List of dictionaries to write
            filename: Output filename

        Returns:
            Path to created file
        """
        output_path = self.output_dir / filename
        logger.info(f"Writing {len(data)} records to {output_path}")

        if not data:
            logger.warning("No data to write")
            # Create empty file with headers
            async with aiofiles.open(output_path, mode="w", newline="", encoding="utf-8") as f:
                await f.write("")
            return output_path

        # Write CSV in memory first, then write to file
        # This is more efficient than writing line by line
        import io

        output = io.StringIO()
        fieldnames = data[0].keys()
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

        csv_content = output.getvalue()

        async with aiofiles.open(output_path, mode="w", encoding="utf-8") as f:
            await f.write(csv_content)

        logger.info(f"Successfully wrote {len(data)} records to {output_path}")
        return output_path
