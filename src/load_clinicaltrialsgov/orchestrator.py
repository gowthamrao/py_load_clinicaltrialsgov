# Copyright (c) 2025-2026 Gowtham Adamane Rao. All Rights Reserved.
#
# Licensed under the Prosperity Public License 3.0.0 (the "License").
# You may not use this file except in compliance with the License.
# You may obtain a copy of the License in the LICENSE file at the root
# of this repository, or at: https://prosperitylicense.com/versions/3.0.0
#
# Commercial use beyond a 30-day trial requires a separate license.


import structlog
import time
from typing import Dict, List, Any
from collections import Counter
from pydantic import ValidationError

from load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from load_clinicaltrialsgov.extractor.api_client import APIClient
from load_clinicaltrialsgov.transformer.transformer import Transformer
from load_clinicaltrialsgov.config import settings
from load_clinicaltrialsgov.models.api_models import Study

logger = structlog.get_logger(__name__)


class Orchestrator:
    """
    Orchestrates the ETL process from extraction to loading.
    """

    TABLE_METADATA: Dict[str, List[str]] = {
        "raw_studies": ["nct_id"],
        "studies": ["nct_id"],
        "sponsors": ["nct_id", "name", "agency_class"],
        "conditions": ["nct_id", "name"],
        "interventions": ["nct_id", "intervention_type", "name"],
        "intervention_arm_groups": ["nct_id", "intervention_name", "arm_group_label"],
        "design_outcomes": ["nct_id", "outcome_type", "measure"],
    }

    def __init__(
        self,
        connector: DatabaseConnectorInterface,
        api_client: APIClient,
        transformer: Transformer,
    ) -> None:
        self.connector = connector
        self.api_client = api_client
        self.transformer = transformer

    def _load_and_clear_batch(self) -> Dict[str, int]:
        """
        Loads the current batch of data from the transformer into the database
        and then clears the transformer's state.
        """
        batch_table_metrics: Dict[str, int] = {}
        dataframes = self.transformer.get_dataframes()
        for table_name, df in dataframes.items():
            if not df.empty:
                record_count_table = len(df)
                batch_table_metrics[table_name] = record_count_table
                logger.info(
                    "loading_data_into_table",
                    table_name=table_name,
                    record_count=record_count_table,
                )
                primary_keys = self.TABLE_METADATA.get(table_name)
                if not primary_keys:
                    logger.error(
                        "no_primary_key_defined_for_table", table_name=table_name
                    )
                    continue

                self.connector.bulk_load_staging(table_name, df)
                self.connector.execute_merge(table_name, primary_keys)

        self.transformer.clear()
        return batch_table_metrics

    def run_etl(self, load_type: str) -> None:
        """
        Executes the full ETL pipeline.
        """
        start_time = time.time()
        log = logger.bind(load_type=load_type, batch_size=settings.etl.batch_size)
        log.info("etl_process_started")

        try:
            # Handle load type specific setup before starting the transaction
            updated_since = None
            if load_type == "full":
                log.info("full_load_initiated_truncating_tables")
                self.connector.truncate_all_tables()
            elif load_type == "delta":
                updated_since = self.connector.get_last_successful_load_timestamp()
                if updated_since:
                    log.info(
                        "delta_load_initiated",
                        updated_since=updated_since.isoformat(),
                    )
                else:
                    log.info("no_successful_load_found_performing_full_load")

            # Start the main transaction for the ETL run
            self.connector.manage_transaction("begin")

            studies_iterator = self.api_client.get_all_studies(
                updated_since=updated_since
            )

            total_record_count = 0
            batch_record_count = 0
            total_table_metrics: Counter[str] = Counter()

            for study_dict in studies_iterator:
                # Extract NCT ID safely before validation
                nct_id = (
                    study_dict.get("protocolSection", {})
                    .get("identificationModule", {})
                    .get("nctId")
                )

                try:
                    # 1. Validate each study individually
                    validated_study = Study.model_validate(study_dict)

                    # 2. Transform if validation is successful
                    self.transformer.transform_study(validated_study, study_dict)

                except ValidationError as e:
                    log.error(
                        "study_validation_failed",
                        nct_id=nct_id,
                        error=str(e),
                    )
                    self.connector.record_failed_study(
                        nct_id=nct_id,
                        payload=study_dict,
                        error_message=f"Pydantic Validation Error: {e}",
                    )
                    continue  # Move to the next study
                except Exception as e:
                    log.error(
                        "study_transformation_failed",
                        nct_id=nct_id,
                        error=str(e),
                        exc_info=True,
                    )
                    self.connector.record_failed_study(
                        nct_id=nct_id,
                        payload=study_dict,
                        error_message=f"Transformation Error: {e}",
                    )
                    continue  # Move to the next study

                total_record_count += 1
                batch_record_count += 1

                if batch_record_count >= settings.etl.batch_size:
                    log.info(
                        "processing_batch",
                        batch_record_count=batch_record_count,
                        total_record_count=total_record_count,
                    )
                    batch_metrics = self._load_and_clear_batch()
                    total_table_metrics.update(batch_metrics)
                    batch_record_count = 0

            # Load any remaining records in the final batch
            if batch_record_count > 0:
                log.info(
                    "processing_final_batch",
                    batch_record_count=batch_record_count,
                    total_record_count=total_record_count,
                )
                final_batch_metrics = self._load_and_clear_batch()
                total_table_metrics.update(final_batch_metrics)

            log.info(
                "finished_processing_studies", total_record_count=total_record_count
            )

            duration = time.time() - start_time
            metrics: Dict[str, Any] = {
                "duration_seconds": round(duration, 2),
                "records_processed": total_record_count,
                "throughput_records_per_sec": (
                    round(total_record_count / duration, 2) if duration > 0 else 0
                ),
                "records_loaded_per_table": dict(total_table_metrics),
            }

            self.connector.record_load_history("SUCCESS", metrics)
            self.connector.manage_transaction("commit")
            log.info("etl_process_completed_successfully", metrics=metrics)

        except Exception as e:
            log.error("etl_process_failed", error=str(e), exc_info=True)
            self.connector.manage_transaction("rollback")
            duration = time.time() - start_time
            metrics = {"error": str(e), "duration_seconds": round(duration, 2)}
            self.connector.record_load_history("FAILURE", metrics)
        finally:
            self.api_client.close()
