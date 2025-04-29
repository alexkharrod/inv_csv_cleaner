#!/usr/bin/env python3
import csv
import logging
import os
import sys
from datetime import datetime

import numpy as np
import pandas as pd

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.expanduser("~/Documents/Inventory_Processing/debug_log.txt")
        ),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def clean_and_save_file(input_filename, output_folder):
    """Process and clean the inventory CSV file."""
    logger.info(f"Starting to process file: {input_filename}")

    try:
        # Skip if this is already a cleaned file
        if "cleaned" in input_filename:
            logger.info("Skipping already cleaned file")
            return

        # Read the CSV with specific encoding and quote handling
        df = pd.read_csv(
            input_filename,
            encoding="cp1252",
            header=0,
            quoting=csv.QUOTE_ALL,
        )
        logger.info(f"Successfully read CSV with {len(df)} rows")

        # Make a copy of the first column to use as index
        # This avoids any issues with in-place modifications
        sku_column = df.iloc[:, 0].copy()

        # Convert to string safely
        sku_column = sku_column.astype(str)

        # Remove quotes safely
        sku_column = sku_column.str.replace('"', "", regex=False)

        # Set as index
        df = df.iloc[:, 1:]  # Remove the first column
        df.index = sku_column
        df.index.name = "SKU"

        # Clean column names (remove quotes)
        df.columns = [str(col).replace('"', "") for col in df.columns]

        logger.info("Set SKU index successfully")

        # List of categories to filter out
        categories = [
            "Uncategorized",
            "Inventory",
            "Total Inventory",
            "Total Uncategorized",
            "TOTAL",
            "Core Stock - ATL",
            "Total Core Stock - ATL",
            "Total Core Stock - ATL\\",
            "Core Stock - ATL\\",
            "Core Stock - ATL, AMS",
            "Total Core Stock - ATL, AMS",
        ]

        # Create simple filter lists for safety
        to_keep = []
        for idx in df.index:
            idx_str = str(idx)

            # Check if it's a category to filter out
            if idx_str in categories:
                continue

            # Check if it matches number patterns
            if idx_str.startswith(
                (
                    "14-",
                    "16-",
                    "20-",
                    "21-",
                    "70-",
                    "00-",
                    "14.",
                    "320-",
                    "IP15L",
                    "IP19L",
                    "Chrome-",
                    "Customs-",
                    "LGE-",
                    "OF15L",
                    "R15L",
                    "RM12L",
                    "W24L",
                    "VGL",
                )
            ):
                continue

            # Check prefixes
            if idx_str.startswith(
                (
                    "BSBI-",
                    "Seneca-",
                    "BF",
                    "OptConnect-",
                    "BrightSign-",
                    "Chrome-",
                    "Customs-",
                    "IP15L",
                    "IP19L",
                )
            ):
                continue

            # Otherwise keep it
            to_keep.append(idx)

        # Filter the dataframe
        original_count = len(df)
        df = df.loc[to_keep]
        filtered_count = original_count - len(df)
        logger.info(f"Filtered out {filtered_count} rows")

        # Process SKU format safely
        new_index = []
        for idx in df.index:
            idx_str = str(idx)
            if " (" in idx_str:
                new_index.append(idx_str.split(" (")[0])
            else:
                new_index.append(idx_str)

        df.index = new_index
        logger.info("Cleaned SKU formats")

        # Create output path
        input_basename = os.path.basename(input_filename)
        base_name = os.path.splitext(input_basename)[0]
        new_filename = os.path.join(output_folder, f"{base_name}_cleaned.csv")
        logger.info(f"Preparing to save to: {new_filename}")

        # Create output directory if needed
        os.makedirs(output_folder, exist_ok=True)

        # Save the cleaned file
        df.to_csv(
            new_filename,
            quoting=csv.QUOTE_MINIMAL,  # Changed to MINIMAL for safety
            encoding="utf-8",
        )
        logger.info("File saved successfully")

        return True

    except Exception as e:
        logger.error(f"Error processing file: {str(e)}")
        import traceback

        logger.error(f"Full error trace: {traceback.format_exc()}")
        return False


def main():
    """Main function to handle script execution."""
    logger.info("Script starting...")

    if len(sys.argv) < 3:
        logger.error("Usage: python3 item_csv_cleaner.py input_file output_folder")
        sys.exit(1)

    input_file = sys.argv[1]
    output_folder = sys.argv[2]

    logger.info(f"Input file: {input_file}")
    logger.info(f"Output folder: {output_folder}")

    success = clean_and_save_file(input_file, output_folder)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
