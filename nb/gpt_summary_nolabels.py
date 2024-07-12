import polars as pl
import pandas as pd
import json
import sys
import concurrent
from tqdm import tqdm
from sqlmodel import Session, select
from langchain_community.vectorstores import Chroma
from langchain.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI
from langchain_community.embeddings import OpenAIEmbeddings
from langchain_core.prompts import PipelinePromptTemplate, PromptTemplate
from langchain.schema.runnable import RunnablePassthrough
from langchain.schema.output_parser import StrOutputParser
from langchain.chains import create_retrieval_chain
from langchain.chains.combine_documents import create_stuff_documents_chain



from openai import OpenAI, AzureOpenAI

def gpt_call(system_prompt, user_prompt, api_key,
              temperature=0, gpt_model = 'gpt-4-1106-preview'):
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
        temperature=temperature
        )
    term = chat_completion.choices[0].message.content
    return term

def rag_call(row, rag_chain):
    condition_name = row['condition_name']
    drug = row['ingredient_name']
    affect = row['affect']

    user_prompt = """"
    Summarize the evidence supporting the relationship between a potential adverse reaction and drug exposure. 
    Include supporting evidence from clinical trials, case studies, and post-marketing surveillance, 
    specifying each source of evidence. Ensure the summary is concise. If there is no known evidence supporting 
    this relationship, state that explicitly.
    Specifically, provide evidence that {} is an adverse reaction of {}.  
    """.format(condition_name, drug)

    support = rag_chain.invoke({'input': user_prompt})
    support_response = support['answer']
    support_docs = [x.metadata['source'] for x in support['context']]
    print(support_docs)

    refute_prompt = """"
    Summarize the evidence, if any, refuting the relationship between a potential adverse reaction and drug 
    exposure.
    Be as concise as possible. If you know the adverse reaction is related to the drug and there is no 
    evidence, just state 'NA'. In that case, no additional information is needed.
    Specifically, provide evidence that there is no causal relationship between {} and {}.
    """.format(condition_name, drug)

    refute = rag_chain.invoke({'input': refute_prompt})
    refute_response = refute['answer']
    refute_docs = [x.metadata['source'] for x in refute['context']]

    
    return [condition_name, drug, affect, support_response, refute_response, support_docs, refute_docs]

def make_prompt(adr, drug, mode = 'basic', supporting = True, label_text = None):

    system_prompt = """"
    You are an expert language model trained to summarize and evaluate medical and scientific evidence. 
    When asked to summarize the evidence supporting the relationship between a potential adverse reaction and 
    drug exposure, focus on providing a clear and concise summary. Include relevant details, if any, from 
    clinical trials, case studies, and post-marketing surveillance. 
    If no evidence is available, clearly state this. 
    Be precise and specific and include the sources of the evidence.
    """
    
    if supporting:
        user_prompt = """"
        Summarize the evidence supporting the relationship between a potential adverse reaction and drug exposure. 
        Include supporting evidence from clinical trials, case studies, and post-marketing surveillance, 
        specifying each source of evidence. Ensure the summary is concise. If there is no known evidence supporting 
        this relationship, state that explicitly.
        Specifically, provide evidence that {} is an adverse reaction of {}.  
        """.format(adr, drug)
    else:
        user_prompt = """"
        Summarize the evidence, if any, refuting the relationship between a potential adverse reaction and drug 
        exposure.
        Be as concise as possible. If you know the adverse reaction is related to the drug and there is no 
        evidence, just state 'NA'. In that case, no additional information is needed.
        Specifically, provide evidence that there is no causal relationship between {} and {}.
        """.format(adr, drug)

    if mode == 'inprompt':
        user_prompt = user_prompt + """
        Here are the relevant sections (adverse reactions, warnings, and box warnings) of the drug label: {}
        """.format(label_text)

    if mode == 'rag':

        system_prompt = (
            """You are an expert language model trained to summarize and evaluate medical and scientific evidence. 
            When asked to summarize the evidence supporting the relationship between a potential adverse reaction and 
            drug exposure, focus on providing a clear and concise summary. Include relevant details, if any, from 
            clinical trials, case studies, and post-marketing surveillance. 
            If no evidence is available, clearly state this. 
            Be precise and specific and include the sources of the evidence.""",
            "Use the following pieces of retrieved context to help answer "
            "the question. If you don't know the answer, say that you "
            "don't know. Keep the answer concise."
            "\n"
            "{context}"
        )

        # user_prompt = ChatPromptTemplate.from_template(user_prompt)
        
    return system_prompt, user_prompt

def get_drug_label_text(my_file):
    data = json.load(open(my_file))
    all_text = ''
    for section in data['sections'].keys():
        if section in ['AR', 'WP', 'BW']:
            all_text += data['sections'][section].replace("\n", "")
    return all_text

def iteration(row, api_key, mode, retriever = None):
    condition_name = row['condition_name']
    drug = row['ingredient_name']
    affect = row['affect']
    myfile = row['file'] # f".{row['file']}"
    all_text = None
    if mode == 'inprompt':
        all_text = get_drug_label_text(myfile)

    if mode != 'rag':
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

    if mode not in ['basic', 'inprompt', 'rag']:
        print('wrong mode')
        sys.exit()

    api_source = 'OpenAI'

    api_key = config[api_source]['openai_api_key'] #constants.AZURE_OPENAI_KEY

    label_dat = (pl.read_csv('../data/labels_w_ADRs.csv')[['condition_name', 'ingredient_name', 'affect', 'file']]
                 .unique()
                 .sort(['condition_name', 'ingredient_name'])
                 )

    if mode == 'rag':
        db = Chroma(persist_directory="../../Chroma", embedding_function=OpenAIEmbeddings(openai_api_key=api_key))
        retriever = db.as_retriever()

        print(len(db.get()['documents']))

        system_prompt = (
            """"
            You are an expert language model trained to summarize and evaluate medical and scientific evidence. 
            When asked to summarize the evidence supporting the relationship between a potential adverse reaction and 
            drug exposure, focus on providing a clear and concise summary. Include relevant details, if any, from 
            clinical trials, case studies, and post-marketing surveillance. 
            If no evidence is available, clearly state this. 
            Be precise and specific and include the sources of the evidence.
            Use the following pieces of retrieved context to answer the query.
            context: '{context}'
            """
        )

        prompt = ChatPromptTemplate.from_messages(
                    [
                        ("system", system_prompt),
                        ("human", "{input}"),
                    ]
                )

        llm = ChatOpenAI(api_key=api_key, model_name="gpt-4-1106-preview", temperature=0)

        question_answer_chain = create_stuff_documents_chain(llm, prompt)
        rag_chain = create_retrieval_chain(retriever, question_answer_chain)

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exec:
            results.extend(list(tqdm(
                exec.map(rag_call,
                        label_dat.rows(named=True),
                        [rag_chain] * len(label_dat.rows())), 
                total=len(label_dat.rows(named=True))
            )))

        # [condition_name, drug, affect, support_response, refute_response, support_docs, refute_docs]
        gpt_output = pd.DataFrame(
            [r for r in results if r is not None],
            # [condition_name, drug, affect, support_response, refute_repsonse]
            columns=['condition_name', 'drug_name', 'affect', 'support_response', 'refute_repsonse',
                     'support_docs', 'refute_docs']
            )

        print(gpt_output.head(1))
        gpt_output.to_csv(f'../results/gpt_{mode}_results_v2.csv', index=False)

    else:

        results = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as exec:
            results.extend(list(tqdm(
                exec.map(iteration,
                        label_dat.rows(named=True),
                        [api_key] * len(label_dat.rows()),
                        [mode] * len(label_dat.rows()),
                        [retriever] * len(label_dat.rows())), 
                total=len(label_dat.rows(named=True))
            )))

        gpt_output = pd.DataFrame(
            [r for r in results if r is not None],
            # [condition_name, drug, affect, support_response, refute_repsonse]
            columns=['condition_name', 'drug_name', 'affect', 'support_response', 'refute_repsonse']
            )

        # print(gpt_output)
        gpt_output.to_csv(f'../results/gpt_{mode}_results_v2.csv', index=False)
        sys.exit()

if __name__ == "__main__":
    main()