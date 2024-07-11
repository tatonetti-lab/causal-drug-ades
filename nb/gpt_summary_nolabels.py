import polars as pl
import pandas as pd
import json
import sys
import concurrent
from tqdm import tqdm
from openai import OpenAI, AzureOpenAI

def gpt_call(system_prompt, user_prompt, api_key,
              temperature=1, gpt_model = 'gpt-4-1106-preview'):
    client = OpenAI(api_key=api_key)
  
    chat_completion = client.chat.completions.create(
        messages=[
            {"role": "system", "content": system_prompt},
            {
                "role": "user",
                "content": user_prompt
            }
        ],
        model=gpt_model,
        temperature=temperature,
    )
    term = chat_completion.choices[0].message.content
    return term

def make_prompt(adr, drug, mode = 'basic', supporting = True, label_text = None):

    system_prompt = """"
    You are an expert language model trained to summarize and evaluate medical and scientific evidence. 
    When asked to summarize evidence supporting the relationship between a potential adverse reaction and 
    drug exposure, focus on providing a clear and concise summary. Include relevant details, if any, from 
    clinical trials, case studies, and post-marketing surveillance. 
    If no evidence is available, clearly state this. 
    Be precise and specific and include the sources of the evidence.
    """
    
    if supporting:
        user_prompt = """"
        Summarize the evidence supporting the relationship between a potential adverse reaction and drug exposure. 
        Specifically, provide evidence that {} is an adverse reaction of {}.  
        Include supporting evidence from clinical trials, case studies, and post-marketing surveillance, 
        specifying each source of evidence. Ensure the summary is concise. If there is no known evidence supporting 
        this relationship, state that explicitly.
        """.format(adr, drug)
    else:
        user_prompt = """"
        Summarize the evidence, if any, refuting the relationship between a potential adverse reaction and drug 
        exposure. Specifically, provide evidence that {} is not an adverse reaction of {}.
        Be as concise as possible. If you know the adverse reaction is related to the drug and there is no 
        evidence, just state 'NA' and no additional information is needed.
        """.format(adr, drug)

    if mode == 'inprompt':
        user_prompt = user_prompt + """
        Here are the relevent sections (adverse reactions, warnings, and box warnings) of the drug label: {}
        """.format(label_text)

    return system_prompt, user_prompt

def get_drug_label_text(my_file):
    data = json.load(open(my_file))
    all_text = ''
    for section in data['sections'].keys():
        if section in ['AR', 'WP', 'BW']:
            all_text += data['sections'][section].replace("\n", "")
    return all_text

def iteration(row, api_key, mode):
    condition_name = row['condition_name']
    drug = row['ingredient_name']
    affect = row['affect']
    myfile = f".{row['file']}"
    all_text = None
    if mode == 'inprompt':
        all_text = get_drug_label_text(myfile)
    support_response = gpt_call(*make_prompt(condition_name, drug,
                                             mode=mode, label_text = all_text), api_key)
    refute_repsonse = gpt_call(*make_prompt(condition_name, drug,
                                            mode=mode, label_text = all_text, supporting=False), api_key)

    return [condition_name, drug, affect, support_response, refute_repsonse]
        
def main():
    config = json.load(open('../config.json'))

    mode = 'basic'
    if len(sys.argv) > 1:
        mode = sys.argv[1]

    api_source = 'OpenAI'

    api_key = config[api_source]['openai_api_key'] #constants.AZURE_OPENAI_KEY

    label_dat = (pl.read_csv('../data/labels_w_ADRs.csv')[['condition_name', 'ingredient_name', 'affect', 'file']]
                 .unique()
                 .sort(['condition_name', 'ingredient_name'])
                 .head(2)
                 )

    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exec:
        results.extend(list(tqdm(
            exec.map(iteration,
                      label_dat.rows(named=True),
                      [api_key] * len(label_dat.rows()),
                      [mode] * len(label_dat.rows())), 
            total=len(label_dat.rows(named=True))
        )))

    gpt_output = pd.DataFrame(
        [r for r in results if r is not None],
        # [condition_name, drug, affect, support_response, refute_repsonse]
        columns=['condition_name', 'drug_name', 'affect', 'support_response', 'refute_repsonse']
        )
    
    gpt_output.to_csv(f'../results/gpt_{mode}_results.csv', index=False)
    sys.exit()

if __name__ == "__main__":
    main()