import pandas as pd
from tqdm import tqdm
import json
import os
from pprint import pprint

class Wax:
    debug = True

    def __init__(self, path, output_dir, prefix, task_id):
        self.task_id = task_id
        self.prefix = prefix
        self.output_dir = output_dir
        self.convert_to_json(path)
        self.get_mapping()
        self.create_manifest()

    def convert_to_json(self, path):
        df = pd.read_excel(path, sheet_name=None, index_col=None, engine="openpyxl")
        data = {}

        for sheet in df:

            data[sheet] = []
            
            df_s = df[sheet]

            df_s = df_s.apply(lambda x: x.to_json(), axis=1)

            for row in df_s:
                row = json.loads(row)
                item = {}
                for field in row:
                    if "Unnamed" in field:
                        continue
                    item[field] = row[field]
                data[sheet].append(item)

        self.metadata = data

        Wax.save(data, f"data/tmp/{self.task_id}/data.json")

    @staticmethod
    def save(data, path):

        os.makedirs(os.path.dirname(path), exist_ok=True)

        with open(path, 'w') as f:
            json.dump(data, f, ensure_ascii=False, indent=4,
                sort_keys=True, separators=(',', ': '))

    def get_mapping(self):

        df = self.metadata
        
        results = {}
        data = df["mapping"]
        for item in data:
            results[item["ラベル"]] = item["term"]
        
        self.mapping = results

        if self.debug:
            pprint(self.mapping)

    def get_conf(self, sheet):
        items_mapping = self.metadata[sheet]

        conf_mapping = {}

        conf = {
            "mapping": conf_mapping
        }

        for item in items_mapping:

            # pprint(items_mapping)
            conf_mapping[item["order"]] = {
                "labels": item["label"].split("|"),
                "term": item["term"]
            }

            if not pd.isnull(item["title"]):
                conf["title"] = item["term"]

            
            if not pd.isnull(item["id"]):
                conf["id"] = item["term"]

        return conf

    def create_manifest(self):
        items = self.metadata["items"]

        conf = self.get_conf("items_mapping")

        # pprint(conf)

        # mappings = self.mapping

        mappings = conf["mapping"]

        manifests = []

        for item in items:

            '''
            for field in item:
                if field not in mappings:
                    continue

                term = mappings[field]

                print(term)

            '''

            iiif_metadata = []

            id = None

            for order in sorted(mappings):
                mapping = mappings[order]
                labels = mapping["labels"]

                for label in labels:
                    if label in item:
                        # item[mapping["term"]] = item[label]
                        # break

                        term = mapping["term"]

                        if term in ["is_public"]:
                            continue

                        iiif_metadata.append({
                            "label": label,
                            "value": item[label],
                            "term": term
                        })

                        if term == conf["id"]:
                            id = item[label]

            pprint(iiif_metadata)

            manifest = {
                "metadata": iiif_metadata
            }

            manifest_path = f"{self.output_dir}/api/iiif/items/{id}/manifest.json"

            '''
            print(manifest_path)

            os.makedirs(os.path.dirname(path), exist_ok=True)
   
            with open(path, 'w') as f:
                json.dump(manifest, f, ensure_ascii=False, indent=4,
                    sort_keys=True, separators=(',', ': '))
            '''

            Wax.save(manifest, manifest_path)

            manifests.append({
                "@id": f"{self.prefix}/api/iiif/items/{id}/manifest.json"
            })

            break

        collection = {
            "@context": "http://iiif.io/api/presentation/2/context.json",
            "@id": f"{self.prefix}/api/iiif/item_sets/{'xxx'}.json",
            "manifests": manifests
        }

        Wax.save(collection, f"{self.output_dir}/api/iiif/item_sets/{'xxx'}.json")

        pass