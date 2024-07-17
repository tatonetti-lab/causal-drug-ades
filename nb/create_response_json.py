import sys
import json
import csv
import pandas as pd

def support_dat(myfile, refute = False):
    data = pd.read_csv(myfile, nrows = 10).fillna('None')
    for_json = []
    for row in data.itertuples():
        if refute:
            for_json.append({
                'content': row.refute_repsonse
            })
        else:
            for_json.append({
                'content': row.support_response
            })
    
    return for_json


def main():
    if len(sys.argv) < 2:
        print('Usage: python create_response_json.py <input_file>')
        sys.exit(1)
    
    myfile = sys.argv[1]
    mode = myfile.split('_')[1]
    # Read in the file
    support_json = support_dat(myfile)
    
    updated_support_json = {'instances': support_json,
                    'parameters': {
                        "outputDimensionality": 256
                        }
                        }

    refute_json = support_dat(myfile, refute = True)
    
    updated_refute_json = {'instances': refute_json,
                    'parameters': {
                        "outputDimensionality": 256
                        }
                        }

    print(updated_support_json.keys())
    with open(f'data/support_{mode}.json', 'w') as f:
        json.dump(updated_support_json, f)

    with open(f'data/refute_{mode}.json', 'w') as f:
        json.dump(updated_refute_json, f)

    sys.exit()

if __name__ == '__main__':
    main()