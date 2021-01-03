"""
Schema Guesser

Reads through a dataset to 'guess' the schema.

Current implementation only lists all the values in a set of fields to
work out the set of symbols for an enumerated type.
"""
from gva.data import Reader
from gva.data.formats import dictset
import json

reader = Reader(
        project='dcsgva-da-prd',
        from_path='dcsgva-da-prd-ai-notebook/02_INTERMEDIATE/VIEWS/NVD_CVE_SUMMARY/%datefolders/')

values = {}

for record in reader:
    
    for k,v in record.items():
        
        if k not in ['CVE', 'CWE', 'publishedDate', 'Description', 'v2.0:vectorString', 'v2.0:baseScore', 'v2.0:exploitabilityScore', 'v2.0:impactScore', 'v3.0:vectorString', 'v3.0:baseScore', 'v3.0:exploitabilityScore', 'v3.0:impactScore']:
    
            e = values.get(k, [])
            if not v in e:
                e.append(v)
                values[k] = e
    
schema = []
for k, v in values.items():
    row = { 'name': k, 'type': 'enum', 'symbols': v }
    schema.append(row)

print(json.dumps(schema, sort_keys=False, indent=4))