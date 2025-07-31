import pandas as pd
import logging
import os
from sklearn.model_selection import train_test_split

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



def load_data(data_url: str) -> pd.DataFrame:
    """ Load data from kaggle dataset url """

    try:
        df = pd.read_csv(data_url)
        logger.debug("Data loaded from %s", data_url)
        return df
    except pd.errors.ParserError as e:
        logger.error("failed to pasre the csv file %s", e)
        raise

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """ Preprocess the data"""

    try:
        df.drop('Unnamed: 12',axis=1,inplace=True)
        logger.debug("Data Preprocess Completed")
        return df
    except KeyError as e:
        logger.error("Missing columns in the dataframe %s",e)
        raise
    except Exception as e:
        logger.error("unexpected error during preprocess %s",e)
        raise
def save_data(train_data : pd.DataFrame,test_data: pd.DataFrame,data_path : str) -> pd.DataFrame:
    """ save the train and test Dataset"""
    try:
        raw_data_path = os.path.join(data_path,"raw")
        os.makedirs(raw_data_path,exist_ok=True)
        train_data.to_csv(os.path.join(raw_data_path,"train.csv"),index = False)
        test_data.to_csv(os.path.join(raw_data_path,"test.csv"),index = False)
        logger.debug("Train adn Test data saved %s",raw_data_path)
    except Exception as e:
        logger.error("unexpected error occuring while saving the data %s",e)
        raise



def main():
    try:
        data_path = "D:/online_Food_feedback_prediction/onlinefoods.csv"
        df = load_data(data_url = data_path)
        final_df = preprocess_data(df = df)
        train_data,test_data = train_test_split(final_df,test_size=0.2,random_state=42)
        save_data(train_data=train_data, test_data=test_data, data_path= "./data")
    except Exception as e:
        logger.error("failed to complete the data ingestion process : %s",e)
        print(f"Error : {e}")

if __name__ == "__main__":
    main()





