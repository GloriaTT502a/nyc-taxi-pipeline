# src/config/settings.py
from __future__ import annotations
import os
from dataclasses import dataclass

@dataclass(frozen=True)
class PipelineSettings:
    """
    Pipeline Configuration 
    Immutable configuration object (frozen=True) to guarantee safety across worker nodes.
    """
    runtime_env: str
    catalog: str
    base_path: str
    shp_path: str
    
    bronze_db: str
    silver_db: str
    gold_db: str
    
    run_id_column: str = "_run_id"
    lineage_column: str = "_input_file"
    h3_resolution: int = 8

    dq_rules_path: str = None

    timezone: str = "America/New_York" 
    

    def resolve_table_path(self, layer: str, table_name: str) -> str:
        db_map = {
            "bronze": self.bronze_db,
            "silver": self.silver_db,
            "gold": self.gold_db
        }
        db = db_map.get(layer)
        if not db: 
            raise ValueError(f"Invalid layer: {layer}. Must be bronze, silver, or gold.") 
        
        if self.catalog: 
            return f"{self.catalog}.{db}.{table_name}"
        return f"{db}.{table_name}"
        

    # ==========================================
    # 2. Exporting global constants and table names (for reference by the Loader)
    # ==========================================

    @classmethod
    def from_env(cls, env_override: str = None, catalog_override: str = None) -> PipelineSettings:
        """
        A unified configuration assembly factory
        - Allows explicit overriding via `override` (for `argparse` to receive Bundle parameters)
        - Otherwise, it degrades to reading OS environment variables
        - Never automatically guess the catalog; it must be explicitly passed in! 
        """
        # Fail-Fast 
        catalog = catalog_override or os.getenv("CATALOG")
        if not catalog:
            raise ValueError("CRITICAL: CATALOG must be provided explicitly! Null values ​​or default guesses are strictly prohibited.") 
        
        runtime_env = (env_override or os.getenv("RUNTIME_ENV", "local")).lower()
        
        # 核心原则：环境决定物理计算位置，Catalog 决定数据存储位置。两者解耦。
        catalog = catalog_override or os.getenv("CATALOG", "")
        
        # 路径推导：依然保留，因为本地开发确实没有 /Volumes，但这只涉及文件路径，不涉及数据权限
        is_cloud = "DATABRICKS_RUNTIME_VERSION" in os.environ

        volume_prefix = f"/Volumes/{catalog}/default" if catalog else "/Volumes/nyc/default" 
        def_base = f"{volume_prefix}/" if is_cloud else "/tmp/nyc_taxi_data/"
        def_shp = f"{volume_prefix}/nyczone/taxi_zones/taxi_zones.shp" if is_cloud else "./tests/fixtures/taxi_zones/taxi_zones.shp"
        
        return cls(
            runtime_env=runtime_env,
            catalog=catalog, 
            base_path=os.getenv("BASE_PATH", def_base),
            shp_path=os.getenv("SHP_PATH", def_shp),
            
            bronze_db=os.getenv("BRONZE_DB", "process_bronze"),
            silver_db=os.getenv("SILVER_DB", "process_silver"),
            gold_db=os.getenv("GOLD_DB", "process_gold"),
            
            h3_resolution=int(os.getenv("H3_RESOLUTION", 8)), 
            dq_rules_path=os.getenv("DQ_RULES_PATH", None)
        ) 

