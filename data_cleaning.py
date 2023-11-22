import pandas as pd 
import re
import numpy as np 

class DataCleaning:


    '''
    A class used to clean data from different sources

    
    Parameters:
    ----------
    None
    
    Attributes:
    ----------
    None

    Methods:
    -------


    format_date(cself,df,date_col)
        helper function to convert string columns in a DataFrame corresponding to date in different formats into datetime columns

    clean_user_data(users_df)
         Performs a number of checks on the user data and removes errors and NULL values

    clean_card_data(cards_df)
         Performs a number of checks on the user data and removes errors and NULL values

    called_clean_store_data(store_df)
        Performs a number of checks on the user data and removes errors and NULL values

    convert_product_weights(products_df)
        Ensures consitency in the unite of measures for the column 'weight' of the products dataframe 

    clean_products_data(products_df)
        Performs a number of checks on the user data and removes errors and NULL values

    clean_orders_data(orders_df)
        Performs a number of checks on the user data and removes errors and NULL values

    clean_sales_data(sales_df)
        Performs a number of checks on the user data and removes errors and NULL values
    '''

    def __init__(self) -> None:
        pass

    def format_date(self,df,date_col):
        
        """
        Parses different date formats and transform them from strings into datetime.
        I identified 3 different possible formats that occurs in different data sources for this project.
        This function is intended to be applied to each date column individually because the different date formats can occur in different rows,
        therefore I could not infer that the same transformation could be applied across all columns for a specific row.

        This method is called in other methods part of this class.
        
        
        Args:
            df (Pandas DataFrame) : DataFrame on which the cnversion to date time is attempted 
            date_col (str) : name of the column to be parsed 

        Returns:
            Pandas DataFrame  
        """
            
        slash_dates = df[df[date_col].str.contains("/", regex=False)]
        wordly_dates = df[df[date_col].str.contains('\d{4}\s[a-zA-Z]+\s\d{2}', regex=True)]
        wordly_dates_reverse = df[df[date_col].str.contains('[a-zA-Z]+\s\d{4}\s\d{2}', regex=True)]
        regular_dates = df[df[date_col].str.contains("\d{4}-\d{2}-\d{2}", regex=True)]

        slash_dates[date_col] = pd.to_datetime(slash_dates[date_col])
        wordly_dates[date_col] = pd.to_datetime(wordly_dates[date_col])
        wordly_dates_reverse[date_col] = pd.to_datetime(wordly_dates_reverse[date_col])
        regular_dates[date_col] = pd.to_datetime(regular_dates[date_col], format='%Y-%m-%d')
        
        print(f"formatted {slash_dates.shape[0]} row with datetime format type '1992/01/12', in column {date_col} ")
        print(f"formatted {wordly_dates.shape[0]} row with datetime format type '1989 October 10',in column {date_col} ")
        print(f"formatted {wordly_dates_reverse.shape[0]} row with datetime format type 'October 1989 10',in column {date_col} ")
        print(f"formatted {regular_dates.shape[0]} row with datetime format type '2005-01-10', in column {date_col} ")

        return pd.concat([slash_dates,wordly_dates,wordly_dates_reverse,regular_dates])
    
    def clean_user_data(self,users_df):

        """
        Performs a number of checks on the user data and removes errors and NULL values
        
        Args:
            users_df (Pandas DataFrame) : DataFrame to clean 

        Returns:
            Pandas DataFrame  
    
        """

        users_df.set_index('index',inplace=True)

        #attempting to drop pandas-recognised NaNs values
        users_df.dropna(inplace=True)

        #lambda replaces elements containing 'NULL' with NaNs, then those can be dropped with .dropna()
        users_df = users_df.apply(lambda x : x[~x.astype('str').str.contains('NULL')]).dropna() 

        # Some of the following conditions appear on the same row, therefore once that row is dropped 
        # the other line of code will do nothing, but I included them anyway, as if working with real data 
        # that may have just 1 condition occurring in a row 

        users_df = users_df[users_df['email_address'].str.contains('@')] # check that all email addresses have an @
        users_df = users_df[~users_df['first_name'].str.contains('\d',regex=True)] #checks names have no numbers
        users_df = users_df[~users_df['last_name'].str.contains('\d',regex=True)] #checks surnames have no numbers
        users_df = users_df[~users_df['country'].str.contains('\d',regex=True)] #checks that country names have no number
        users_df['country_code']=users_df.country_code.str.replace('GGB','GB') #make UK code consistent 

        users_df = self.format_date(users_df,'date_of_birth')
        users_df = self.format_date(users_df,'join_date')

        return users_df


    def clean_card_data(self, cards_df):

        """
        Performs a number of checks on the user data and removes errors and NULL values
        
        Args:
            cards_df (Pandas DataFrame) : DataFrame to clean 

        Returns:
            Pandas DataFrame  
    
        """
        
        #the following lines can extract card numbers from entries with format "??4654488694" - with 1 or more question marks to remove 
        cards_df.dropna(inplace=True)
        cards_question_mark = cards_df[cards_df['card_number'].str.contains('?', regex=False)]
        mark_idx = cards_question_mark.index
        extract = cards_question_mark['card_number'].str.extract('[?]+(\d+)')
        cards_df.loc[mark_idx,'card_number'] = extract[0]

        #lambda replaces elements containing 'NULL' with NaNs, then those can be dropped with .dropna()
        cards_df = cards_df.apply(lambda x : x[~x.astype('str').str.contains('NULL')]).dropna()
        cards_df = self.format_date(cards_df,'date_payment_confirmed')
        cards_df = cards_df[~cards_df['card_number'].str.contains('[a-zA-Z]',regex=True)]

        return cards_df
    
    def called_clean_store_data(self, store_df):

        """
        Performs a number of checks on the user data and removes errors and NULL values
        
        Args:
            store_df (Pandas DataFrame) : DataFrame to clean 

        Returns:
            Pandas DataFrame  
    
        """

        store_df = store_df.dropna(how='all',axis=0)
        store_df = self.format_date(store_df,'opening_date')
        store_df = store_df[~store_df['country_code'].str.contains('\d',regex=True)]
        store_df = store_df[~store_df['continent'].str.contains('\d',regex=True)]
        store_df['continent'] = store_df.continent.str.replace('ee','')
        store_df = store_df.replace(value=None,to_replace='NULL|N/A', regex=True)

        staff_numbers = store_df['staff_numbers'].dropna()
        staff_numbers = staff_numbers[staff_numbers.str.contains('[A-Za-z]')]
        extraction = staff_numbers.str.extract('[A-Za-z]*(\d*)[A-Za-z]*(\d*)[A-Za-z]*').sum(axis=1)
        store_df.loc[extraction.index,'staff_numbers'] = extraction

        store_df[['lat',
                  'latitude',
                  'longitude',
                  'staff_numbers']] = (store_df[['lat',
                                                 'latitude',
                                                 'longitude',
                                                 'staff_numbers']]
                                                .fillna('0')
                                                .astype('float'))

        store_df = store_df[['address', 'longitude', 'locality', 'store_code',
            'staff_numbers', 'opening_date', 'store_type', 'latitude',
            'country_code', 'continent']]
                


        return store_df

    def convert_product_weights(self,products_df):

        """
        Peforms transformations on the weight columns to convert different unit of weights and volume into KGs,
        weights described in terms of individual weights of constituents of items sold in bulk is also transformed into the total weight in KGs.

        Args:
            products_df (Pandas DataFrame) : DataFrame to process 

        Returns:
            Pandas DataFrame  
    
        """
 
        # some products are sold in bulk and the weight is expressed as:
        # item_weight*number_of_items

        products_df['weight'] = products_df['weight'].str.replace('(\d*)\s*x\s*(\d*).*([a-zA-Z]).*',
                                  lambda x: str(float(x[1])*float(x[2]))+str(x[3]),
                                  regex=True,)
         
        #conversion for kg, g, oz and ml to kg

        unit_conversion_dict = {'^(\d+[.]*\d*)\s*kg$' : 1,
                              '^(\d+[.]*\d*)\s*g$' : 0.001,
                              '^(\d+[.]*\d*)\s*ml$' : 0.001,
                              '^(\d+[.]*\d*)\s*oz$' : 0.0283}

        for unit in unit_conversion_dict:

            extraction = products_df['weight'].str.extract(unit,expand=False, flags=re.IGNORECASE).dropna()
            idx = extraction.index
            products_df.loc[idx,'weight_kg'] = extraction.astype('float')*unit_conversion_dict[unit]

        
        return products_df
    
    def clean_products_data(self,products_df):

        """
        Performs a number of checks on the user data and removes errors and NULL values
        
        Args:
            products_df (Pandas DataFrame) : DataFrame to clean 

        Returns:
            Pandas DataFrame  
    
        """
         
        products_df.dropna(inplace=True)
        products_df = self.format_date(products_df,'date_added')
        products_df = products_df[~products_df['removed'].str.contains('\d')]
        products_df = products_df[products_df['EAN'].str.contains('\d')]
        products_df = products_df[products_df['product_price'].str.contains('£\d+[.]*\d*')]
        #print(products_df.columns)

        return products_df
    
    def clean_orders_data(self,orders_df):

        """
        Performs a number of checks on the user data and removes errors and NULL values
        
        Args:
            orders_df (Pandas DataFrame) : DataFrame to clean 

        Returns:
            Pandas DataFrame  
    
        """


        orders_df.drop(columns=['first_name', 'last_name','1','level_0'],inplace=True)
        orders_df.set_index('index', inplace=True)
            
        return orders_df

    def clean_sales_data(self,sales_df):

        """
        Performs a number of checks on the user data and removes errors and NULL values
        
        Args:
            sales_df (Pandas DataFrame) : DataFrame to clean 

        Returns:
            Pandas DataFrame  
    
        """

        sales_df = sales_df.apply(lambda x : x[~x.astype('str').str.contains('NULL')]).dropna()
        
        sales_df = sales_df[~sales_df['year'].str.contains('[a-zA-Z]')]
        sales_df = sales_df[~sales_df['month'].str.contains('[a-zA-Z]')]
        sales_df = sales_df[~sales_df['day'].str.contains('[a-zA-Z]')]
        sales_df = sales_df[~sales_df['time_period'].str.contains('\d')]
   
        return sales_df


         

         


