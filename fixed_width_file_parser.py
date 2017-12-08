record_splits = [(1, 9),
    (10, 15),
    (16, 16),
    (17, 18),
    (19, 19),
    (20, 24),
    (25, 32),
    (33, 47),
    (48, 51),
    (52, 91),
    (92, 96)
    ]

schema = [list_Of_Columns]

def split_lines(strng):
    """takes input the string/line that it parses and the split which will parse the recors
    """
    return [strng[k[0]-1:k[1]] for k in record_splits]


def create_data(file_path, schema):
    """takes input the fixed width file we need to parse and schema
       spits out the dataframe which has got columns as we desire
    """
    rdd = sc.textFile(file_path)
    df = rdd.map(split_lines).toDF(schema)
    return df

final_df = create_data("/path/to/file",schema)
final_df.write.parquet("/path/to/output_location")
