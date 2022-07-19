S3 Writer (Hlidac Shopu)
=============

Ze vstupni tabulky / tabulek vygeneruje json soubory `$shop.tld$/$slug$/price-history.json`
nebo `$shop.tld$/$slug$/metadata.json`


[TOC]


Configuration
=============

Accepts following parameters:


- AWS access key ID `aws_access_key_id` 
- AWS secret access key `#aws_secret_access_key`
- Input file format `format` (values `metadata` or `pricehistory`)
- Target AWS bucket `aws_bucket`
- AWS directory name `aws_directory` (only if needed)
- Number of threads `workers` (sets the number of threads to be used, cpu bound)
- Batch (chunk) size `chunksize`

Kazda vsuptni tabulka musi obsahovat sloupce `shop_id` a `slug`.

### Price History

**Konfigurace - příklad**

```json
{
  "storage": {
    "input": {
          "files": []
    }
  },
  "parameters": {
    "format": "pricehistory",
    "aws_access_key_id": "XXXXXXXXXXXXXXXXXXXX",
    "#aws_secret_access_key": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "aws_bucket": "keboola-test",
    "aws_directory": "s3_test/",
    "workers": 32,
    "chunksize": 5000
  },
  "action":"run",
  "authorization": {}
}
```

Ocekava tabulku se sloupci `shop_id`, `slug` a `json`

JSON string ve sloupci `json` se ulozi podle nasledujici masky:
`$shop.tld$/$slug$/price-history.json`

### Metadata

**Konfigurace**

```json
{
  "storage": {
    "input": {
          "files": []
    }
  },
  "parameters": {
    "format": "metadata",
    "aws_access_key_id": "XXXXXXXXXXXXXXXXXXXX",
    "#aws_secret_access_key": "XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX",
    "aws_bucket": "keboola-test",
    "aws_directory": "s3_test/",
    "workers": 32,
    "chunksize": 5000
  },
  "action":"run",
  "authorization": {}
}
```

Ocekava tabulku minimanlne se sloupci `shop_id`, `slug`.

Ostatni sloupce se pouziji jako JSON atributy. Vysledek se ulozi podle nasledujici masky:
`$shop.tld$/$slug$/metadata.json`

# Development

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to your custom path in
the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone git@bitbucket.org:kds_consulting_team/kds-team.processor-json-generator-hlidac-shopu.git kds-team.processor-json-generator-hlidac-shopuk
cd kds-team.processor-json-generator-hlidac-shopuk
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/)
