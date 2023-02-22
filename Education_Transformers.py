"""
Compute the Semantic Textual Similarity between two text files.
In this use case, we want to map a structured categorization of educational degrees in Italy to manually free_text data in
a free field form.

e.g. "Scienze economico-aziendali" will be mapped to "Economia Aziendale", "Laurea in Economia", ...
"""
import pandas as pd
import torch
from sentence_transformers import SentenceTransformer, util

# Load the data from free free_text text
free_text = pd.read_json("./data/education_mapping.json", orient='index', typ='series')

# Targets
edu_fields = pd.read_excel(r"./data/Codifiche.xlsx", sheet_name="Titolo e Materia di studio")
edu_fields = edu_fields.drop(columns=['Modificato'])
edu_fields.fillna(method='ffill', inplace=True)
assert (edu_fields['Titolo di studio'].unique() == free_text.index.values).all()

# We try out this model https://huggingface.co/efederici/sentence-it5-base
# Alternatively, there is https://huggingface.co/efederici/sentence-bert-base but is worse
model = SentenceTransformer('efederici/sentence-IT5-base', device='cuda:0')

# Run inference for each educational level
sentences = []
targets = []
for edu in free_text.index:
    s = free_text[edu]
    t = edu_fields.loc[edu_fields["Titolo di studio"] == edu, 'Materia di studio'].to_numpy()

    # Compute embeddings
    embeddings_sentences = model.encode(s, convert_to_tensor=True)
    embeddings_target = model.encode(t, convert_to_tensor=True)

    # Compute cosine-similarities for each sentence with each target
    cosine_scores = util.cos_sim(embeddings_sentences, embeddings_target)

    # Find the pairs with the highest cosine similarity scores
    idx = torch.argmax(cosine_scores, dim=1)

    # Save the sentence and target
    sentences.extend(s)
    targets.extend(t[idx.cpu().numpy()])

    print(edu)

# Save results
results = pd.DataFrame(data={'sentences': sentences, 'target': targets})
results.to_excel("./data/education_mapping_new.xlsx")
