#!/bin/bash

for i in support refute;
do
    for j in basic inprompt; # rag;
    do
        echo "PULLING FROM: data/${i}_${j}.json"
        echo "SAVING TO: results/${i}_${j}_embeds.json"
        curl -X POST \
            -H "Authorization: Bearer $(gcloud auth print-access-token)" \
            -H "Content-Type: application/json; charset=utf-8" \
            "https://us-central1-aiplatform.googleapis.com/v1/projects/refsides/locations/us-central1/publishers/google/models/text-embedding-004:predict" \
            -d @data/${i}_${j}.json > results/${i}_${j}_embeds.json
    done
done


