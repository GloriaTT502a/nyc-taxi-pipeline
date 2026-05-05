import os 
import glob 
from pyspark.sql import SparkSession 

from common.logger import get_logger 

# Get current script logger 
logger = get_logger(__name__)

def main(): 
    # Get current spark env 
    spark = SparkSession.builder.appName("Database_Migrations").getOrCreate()

    # Location to migration folder 
    # Use __file__ to find the current abstract location 
    current_dir = os.path.dirname(os.path.abspath(__file__))

    # Find out all .sql files and order by the first character of file name
    sql_files = glob.glob(os.path.join(current_dir, "*.sql"))
    sql_files.sort() 

    if not sql_files: 
        logger.warning("Cannot find any migration SQL script") 
        return 
    
    logger.info(f"Start to process migration scripts. Found {len(sql_files)} scritps")

    # Run SQL 
    for file_path in sql_files: 
        file_name = os.path.basename(file_path) 
        logger.info(f"[{file_name}] is runing")

        try: 
            with open(file_path, 'r', encoding='utf-8') as f: 
                sql_content = f.read() 

            # Fault tolerance: separate command by ";" to make sure only one CREATE for each command. 
            statements = [s.strip() for s in sql_content.split(',') if s.strip()]

            for statement in statements: 
                spark.sql(statement) 

            logger.info(f"[{file_name}] is finised") 
                  
        except Exception as e: 
            logger.error(f"[{file_name}] has an error: {e}") 
            raise e 
    
    logger.info("All migration scripts are finished!") 

if __name__ == "__main__": 
    main()

    


