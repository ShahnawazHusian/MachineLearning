import pandas as pd
import logging
import os
from sklearn.preprocessing import LabelEncoder

log_dir = "logs"
os.makedirs(log_dir,exist_ok = True)

logger = logging.getLogger("data_ingestion")
logger.setLevel("DEBUG")

console_handler = logging.StreamHandler()
console_handler.setLevel("DEBUG")

log_file_path = os.path.join(log_dir,"data_ingestion.log")
file_handler = logging.FileHandler(log_file_path)
file_handler.setLevel("DEBUG")

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console_handler.setFormatter(formatter)
file_handler.setFormatter(formatter)

logger.addHandler(console_handler)
logger.addHandler(file_handler)


def preprocess_df(df: pd.DataFrame) -> pd.DataFrame:
    """ Preprocessing the DataFrame by encoding the traget columns  """
    try:
        logger.debug("starting preprocessing for DataFrame")
        encoder1=LabelEncoder()
        encoder2=LabelEncoder()
        encoder3=LabelEncoder()
        encoder4=LabelEncoder()
        encoder5=LabelEncoder()
        encoder6=LabelEncoder()
        encoder7=LabelEncoder()
        df['Gender']=encoder1.fit_transform(df['Gender'])
        df['Marital Status']=encoder2.fit_transform(df['Marital Status'])
        df['Occupation']=encoder3.fit_transform(df['Occupation'])
        df['Educational Qualifications']=encoder4.fit_transform(df['Educational Qualifications'])
        df['Output']=encoder5.fit_transform(df['Output'])
        df['Feedback']=encoder6.fit_transform(df['Feedback'])
        df['Monthly Income']=encoder7.fit_transform(df['Monthly Income'])
        logger.debug("Target columns encoded")
        return df
        
    except KeyError as e:
        logger.error("column not found %s",e)
        raise    
    except Exception as e:
        logger.error("Error during encoding %s", e)
        raise

def main():
    """ Main function to load raw data , process it and save processed data """
    try:
        train_data = pd.read_csv("./data/raw/train.csv")
        test_data = pd.read_csv("./data/raw/test.csv")
        logger.debug("Data loaded properly")
        
        train_processed_data = preprocess_df(train_data)
        test_processed_data = preprocess_df(test_data)

        data_path = os.path.join("./data","intrim")
        os.makedirs(data_path,exist_ok=True)

        train_processed_data.to_csv(os.path.join(data_path,"train_processed_data"),index = False)
        test_processed_data.to_csv(os.path.join(data_path,"test_processed_data"),index = False)

        logger.debug("processed data saved %s",data_path)

    except FileNotFoundError as e:
        logger.error("file not found %s",e)
        raise
    except pd.errors.EmptyDataError as e:
        logger.error("No data %s",e)
        raise
    except Exception as e:
        logger.error("failed to complete the data transformation process : %s",e)
        print(f"Error : {e}")

if __name__ == "__main__":
    main()