import pandas as pd 

class DataCleaning:

    def __init__(self) -> None:
        pass
    
    def clean_user_data(self,users_df):

        def format_date(df,date_col):
        
            """This function can format different date formats. I identified 3 different possible formats
            The two date columns can contain different formats in each row. 
            Therefore the function is applied to both columns separetely"""

            
            slash_dates=df[df[date_col].str.contains("/", regex=False)]
            wordly_dates=df[df[date_col].str.contains('\d{4}\s[a-zA-Z]+\s\d{2}', regex=True)]
            regular_dates=df[df[date_col].str.contains("\d{4}-\d{2}-\d{2}", regex=True)]

            slash_dates[date_col]=pd.to_datetime(slash_dates[date_col])
            wordly_dates[date_col]=pd.to_datetime(wordly_dates[date_col])
            regular_dates[date_col]=pd.to_datetime(regular_dates[date_col])
            
            print(f"formatted {slash_dates.shape[0]} row with datetime format type '/', in column {date_col} ")
            print(f"formatted {wordly_dates.shape[0]} row with datetime format type '1989 October 10',in column {date_col} ")
            print(f"formatted {regular_dates.shape[0]} row with datetime format type '2005-01-10', in column {date_col} ")

            return pd.concat([slash_dates,wordly_dates,regular_dates])

        users_df.set_index('index',inplace=True)

        #attempting to drop pandas-recognised NaNs values
        users_df.dropna(inplace=True)

        #lambda replaces elements containing 'NULL' with NaNs, then those can be dropped with .dropna()
        users_df=users_df.apply(lambda x : x[~x.astype('str').str.contains('NULL')]).dropna() 

        # Some of the following conditions appear on the same row, therefore once that row is dropped 
        # the other line of code will do nothing, but I included them anyway, as if working with real data 
        # that may have just 1 condition occurring in a row 

        users_df=users_df[users_df['email_address'].str.contains('@')] # check that all email addresses have an @
        users_df=users_df[~users_df['first_name'].str.contains('\d',regex=True)] #checks names have no numbers
        users_df=users_df[~users_df['last_name'].str.contains('\d',regex=True)] #checks surnames have no numbers
        users_df=users_df[~users_df['country'].str.contains('\d',regex=True)] #checks that country names have no number
        users_df['country_code']=users_df['country_code']=users_df.country_code.str.replace('GGB','GB') #make UK code consistent 

        users_df=format_date(users_df,'date_of_birth')
        users_df=format_date(users_df,'join_date')

        return users_df

