import sys
import polars as pl
import pandas as pd

from sentence_transformers import SentenceTransformer


def main():

    # get model name from input arguments
    if len(sys.argv) < 3:
        print('Usage: python get_embeddings.py <model_num> <input_file>')
        print('where <model_num> is 0 for llmrails/ember-v1 and 1 for Salesforce/SFR-Embedding-Mistral.')
        print('and <input_file> is the path to the file containing the summaries')
        sys.exit(1)

    if int(sys.argv[1]) not in [0,1]:
        print('Currently only supporting (0) llmrails/ember-v1 and (1) Salesforce/SFR-Embedding-Mistral. Please choose 0 or 1.')
        sys.exit(1)

    model_name = ['llmrails/ember-v1', 'Salesforce/SFR-Embedding-Mistral'][int(sys.argv[1])]


    input_file = sys.argv[2]
    mode = input_file.split('_')[1]
    # Load the model
    model = SentenceTransformer(model_name)
    # Load the data
    # condition_name', 'drug_name', 'affect', 'support_response', 'refute_repsonse'
    df = pd.read_csv(input_file).fillna({'affect': -1})
    df_filtered = df[df['affect'] != 0]
    print(df_filtered['affect'].value_counts())
    data = df_filtered.groupby(['condition_name', 'affect']).apply(lambda x: x.sample(10)).reset_index(drop=True)

    # Get the embeddings
    support_embeddings = model.encode(data['support_response'].to_list())
    refute_embeddings = model.encode(data['support_response'].to_list())
    # Convert the embeddings to a DataFrame
    embeddings = pd.concat([data[['condition_name', 'drug_name', 'affect']],
                             pd.DataFrame(support_embeddings),
                             pd.DataFrame(refute_embeddings)], axis=1)
    # Add the embeddings to the original data
    # Save the data
    print(f'results/{mode}_embeddings.csv')
    embeddings.to_csv(f'results/{mode}_embeddings.csv')
    return None

if __name__ == '__main__':
    main()
    sys.exit()