#!/usr/bin/env python3
import csv
import logging
import os
import sys
from datetime import datetime

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
            header=0,  # Changed from header=1 as requested
            quoting=csv.QUOTE_ALL,
        )
        logger.info(f"Successfully read CSV with {len(df)} rows")

        # Set the first unnamed column as index
        df.set_index(df.columns[0], inplace=True)
        df.index.name = "SKU"
        logger.info("Set SKU index successfully")

        # Remove quotes from all string values
        df.index = df.index.str.replace('"', "")
        for col in df.columns:
            if df[col].dtype == "object":
                df[col] = df[col].str.replace('"', "")
        logger.info("Removed quotes from all fields")

        # Create masks for filtering
        category_mask = (
            (df.index == "Uncategorized")
            | (df.index == "Inventory")
            | (df.index == "Total Inventory")
            | (df.index == "Total Uncategorized")
            | (df.index == "TOTAL")
        )

        number_mask = df.index.str.match(r"^(14|16|20|21|70)-")

        prefix_mask = (
            df.index.str.startswith("BSBI-")
            | df.index.str.startswith("Seneca-")
            | df.index.str.startswith("BF")
            | df.index.str.startswith("OptConnect-")
        )

        # Apply all filters
        original_count = len(df)
        df = df[~(category_mask | number_mask | prefix_mask)]
        filtered_count = original_count - len(df)
        logger.info(f"Filtered out {filtered_count} rows")

        # Split SKUs at parentheses
        df.index = pd.Index([x.split(" (")[0] for x in df.index])
        logger.info("Cleaned SKU formats")

        # Ensure numeric columns are properly formatted
        numeric_columns = [
            col
            for col in df.columns
            if any(x in col.lower() for x in ["days", "current", "total"])
        ]
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Create output path
        input_basename = os.path.basename(input_filename)
        base_name = os.path.splitext(input_basename)[0]
        new_filename = os.path.join(output_folder, f"{base_name}_cleaned.csv")
        logger.info(f"Preparing to save to: {new_filename}")

        # Create output directory if needed
        os.makedirs(output_folder, exist_ok=True)

        # Save the cleaned file
        df.to_csv(
            new_filename, quoting=csv.QUOTE_NONE, escapechar="\\", encoding="utf-8"
        )
        logger.info("File saved successfully")

        # Log summary statistics
        if "TOTAL" in df.columns:
            logger.info(f"Summary Statistics:")
            logger.info(f"Total SKUs processed: {len(df)}")
            logger.info(f"Total inventory value: {df['TOTAL'].sum():,.2f}")
            if "Average Days in Stock" in df.columns:
                logger.info(
                    f"Average days in stock: {df['Average Days in Stock'].mean():.1f}"
                )

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
