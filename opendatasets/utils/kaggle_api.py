import os
from opendatasets.utils.kaggle_direct import get_kaggle_dataset_id, is_kaggle_url
from opendatasets.utils.archive import extract_archive
import click
import json


def _get_kaggle_key(kaggle_key):
    if kaggle_key.startswith('{'):
        try:
            import json
            api_details = json.loads(kaggle_key)
            return api_details['key']
        except Exception:
            return kaggle_key
    return kaggle_key


def read_kaggle_creds():
    try:
        if os.path.exists('./kaggle.json'):
            with open('./kaggle.json', 'r') as f:
                key = f.read()
                data = json.loads(key)
                if 'username' in data and 'key' in data:
                    os.environ['KAGGLE_USERNAME'] = data['username']
                    os.environ['KAGGLE_KEY'] = data['key']
                    return True
    except Exception:
        return False


def download_kaggle_dataset(dataset_url, data_dir, force=False, dry_run=False, username = "", kaggle_key = ""):
    dataset_id = get_kaggle_dataset_id(dataset_url)
    id = dataset_id.split('/')[1]
    target_dir = os.path.join(data_dir, id)

    if not force and os.path.exists(target_dir) and len(os.listdir(target_dir)) > 0:
        print('Skipping, found downloaded files in "{}" (use force=True to force download)'.format(
            target_dir))
        return target_dir

    if not read_kaggle_creds():
        print("Please provide your Kaggle credentials to download this dataset. Learn more: http://bit.ly/kaggle-creds")
        os.environ['KAGGLE_USERNAME'] = username
        os.environ['KAGGLE_KEY'] = _get_kaggle_key(kaggle_key)

    if not dry_run:
        from kaggle import api
        api.authenticate()
        if dataset_id.split('/')[0] == 'competitions' or dataset_id.split('/')[0] == 'c':
            api.competition_download_files(
                id,
                target_dir,
                force=force,
                quiet=False)
            zip_fname = target_dir + '/' + id + '.zip'
            extract_archive(zip_fname, target_dir)
            try:
                os.remove(zip_fname)
            except OSError as e:
                print('Could not delete zip file, got' + str(e))
        else:
            api.dataset_download_files(
                dataset_id,
                target_dir,
                force=force,
                quiet=False,
                unzip=True)

    else:
        print("This is a dry run, skipping..")
    return data_dir
