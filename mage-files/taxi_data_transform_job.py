import pandas as pd
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(df, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        df: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here
    df['tpep_pickup_datetime']= pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime']= pd.to_datetime(df['tpep_dropoff_datetime'])

    df = df.drop_duplicates().reset_index(drop=True)

    datetime_dim = df[['tpep_pickup_datetime','tpep_dropoff_datetime']].reset_index(drop=True)
    datetime_dim= pd.Series(datetime_dim.to_numpy().ravel(order='F'), name='datetime')
    datetime_dim= datetime_dim.drop_duplicates().reset_index(drop=True)
    datetime_dim=pd.DataFrame(datetime_dim)

    datetime_dim['datetime'] = datetime_dim['datetime']
    datetime_dim['hour'] = datetime_dim['datetime'].dt.hour
    datetime_dim['day'] = datetime_dim['datetime'].dt.day
    datetime_dim['month'] = datetime_dim['datetime'].dt.month
    datetime_dim['year'] = datetime_dim['datetime'].dt.year
    datetime_dim['weekday'] = datetime_dim['datetime'].dt.weekday
    datetime_dim['datetime_key'] = datetime_dim.index
    datetime_dim = datetime_dim[['datetime_key','datetime','hour','day','month','year','weekday']]

    rate_code_type = {
        1:"Standard rate",
        2:"JFK",
        3:"Newark",
        4:"Nassau or Westchester",
        5:"Negotiated fare",
        6:"Group ride"
    }

    rate_code_dim = df[['RatecodeID']].reset_index(drop=True)
    rate_code_dim = rate_code_dim.drop_duplicates().reset_index(drop=True)
    rate_code_dim['rate_code_id'] = rate_code_dim['RatecodeID']
    rate_code_dim['rate_code_key'] = rate_code_dim.index
    rate_code_dim['rate_code_name'] = rate_code_dim['rate_code_id'].map(rate_code_type)
    rate_code_dim = rate_code_dim[['rate_code_key','rate_code_id','rate_code_name']]

    pickup_location = df[['pickup_longitude','pickup_latitude']].reset_index(drop=True)
    pickup_location['combined_col'] = pickup_location['pickup_longitude'].astype(str)+ '~~~' + pickup_location['pickup_latitude'].astype(str)
    pickup_location = pickup_location['combined_col']

    dropoff_location = df[['dropoff_longitude','dropoff_latitude']].reset_index(drop=True)
    dropoff_location['combined_col'] = dropoff_location['dropoff_longitude'].astype(str)+ '~~~' + dropoff_location['dropoff_latitude'].astype(str)
    dropoff_location = dropoff_location['combined_col']

    location = []
    location = pd.concat([pickup_location, dropoff_location])
    location= pd.DataFrame(location)
    location['location_key']= location.index

    location_dim= pd.DataFrame(columns=['location_key','location_longitude', 'location_latitude'])
    location_dim[['location_longitude', 'location_latitude']] = location['combined_col'].str.split('~~~', expand=True)
    location_dim= location_dim.drop_duplicates().reset_index(drop=True)
    location_dim['location_key'] = location_dim.index
    location_dim['location_longitude']= location_dim['location_longitude'].astype('float64')
    location_dim['location_latitude']= location_dim['location_latitude'].astype('float64')

    payment_type_name = {
        1:"Credit card",
        2:"Cash",
        3:"No charge",
        4:"Dispute",
        5:"Unknown",
        6:"Voided trip"
    }
    payment_type_dim = df[['payment_type']].reset_index(drop=True)
    payment_type_dim = payment_type_dim.drop_duplicates().reset_index(drop=True)
    payment_type_dim['payment_type_key'] = payment_type_dim.index
    payment_type_dim['payment_type_name'] = payment_type_dim['payment_type'].map(payment_type_name)
    payment_type_dim = payment_type_dim[['payment_type_key','payment_type','payment_type_name']]

    fact_table = df.merge(datetime_dim, left_on ='tpep_pickup_datetime', right_on='datetime')\
    .merge(datetime_dim, left_on='tpep_dropoff_datetime', right_on='datetime')\
    .merge(location_dim, left_on=['pickup_longitude','pickup_latitude'], right_on=['location_longitude','location_latitude'])\
    .merge(location_dim, left_on=['dropoff_longitude','dropoff_latitude'], right_on=['location_longitude','location_latitude'])\
    .merge(rate_code_dim, left_on='RatecodeID', right_on='rate_code_id')\
    .merge(payment_type_dim, left_on='payment_type', right_on='payment_type')\
    [['VendorID','datetime_key_x','datetime_key_y', 'location_key_x','location_key_y','rate_code_key','payment_type_key','passenger_count','trip_distance','store_and_fwd_flag','fare_amount','extra','mta_tax','tip_amount','tolls_amount','improvement_surcharge','total_amount']]
    fact_table = fact_table.rename(columns={'datetime_key_x':'pickup_dt_key','datetime_key_y':'dropoff_dt_key','location_key_x':'pickup_location_key','location_key_y':'dropoff_location_key'})


    
    return {
        'datetime_dim':datetime_dim.to_dict(orient='dict'),
        'location_dim':location_dim.to_dict(orient='dict'),
        'rate_code_dim':rate_code_dim.to_dict(orient='dict'),
        'payment_type_dim':payment_type_dim.to_dict(orient='dict'),
        'fact_table':fact_table.to_dict(orient='dict')
    }


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
