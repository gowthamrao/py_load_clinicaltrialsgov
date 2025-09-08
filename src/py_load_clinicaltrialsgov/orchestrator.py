import structlog
import time
from typing import Dict, List, Any, Literal

from py_load_clinicaltrialsgov.connectors.interface import DatabaseConnectorInterface
from py_load_clinicaltrialsgov.extractor.api_client import APIClient
from py_load_clinicaltrialsgov.transformer.transformer import Transformer

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

    def run_etl(self, load_type: str) -> None:
        """
        Executes the full ETL pipeline.
        """
        start_time = time.time()
        log = logger.bind(load_type=load_type)
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

            record_count = 0
            for study in studies_iterator:
                nct_id = (
                    study.protocol_section.identification_module.get("nctId")
                    if study.protocol_section.identification_module
                    else None
                )
                try:
                    self.transformer.transform_study(study)
                    record_count += 1
                    if record_count % 100 == 0:
                        log.info("processed_studies_batch", record_count=record_count)
                except Exception as e:
                    log.error(
                        "study_transformation_failed",
                        nct_id=nct_id,
                        error=str(e),
                        exc_info=True,
                    )
                    if nct_id:
                        self.connector.record_failed_study(
                            nct_id=nct_id,
                            payload=study.model_dump(),
                            error_message=str(e),
                        )

            log.info("finished_processing_studies", total_record_count=record_count)

            table_metrics = {}
            dataframes = self.transformer.get_dataframes()
            for table_name, df in dataframes.items():
                if not df.empty:
                    record_count_table = len(df)
                    table_metrics[table_name] = record_count_table
                    log.info(
                        "loading_data_into_table",
                        table_name=table_name,
                        record_count=record_count_table,
                    )
                    primary_keys = self.TABLE_METADATA.get(table_name)
                    if not primary_keys:
                        log.error(
                            "no_primary_key_defined_for_table", table_name=table_name
                        )
                        continue

                    self.connector.bulk_load_staging(table_name, df)
                    self.connector.execute_merge(table_name, primary_keys)

            duration = time.time() - start_time
            metrics: Dict[str, Any] = {
                "duration_seconds": round(duration, 2),
                "records_processed": record_count,
                "throughput_records_per_sec": (
                    round(record_count / duration, 2) if duration > 0 else 0
                ),
                "records_loaded_per_table": table_metrics,
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
