# Data licence

`products.jsonl` is a subset derived from two open databases, both licensed under the
[Open Database License (ODbL) 1.0](https://opendatacommons.org/licenses/odbl/1-0/):

- [Open Food Facts](https://world.openfoodfacts.org/) (product names, brands, quantities, ingredients, palm-oil
  status), France subset.
- [Open Prices](https://prices.openfoodfacts.org/) (one median price per product, converted from euros at a fixed
  rate).

This derived file is shared under the same licence. `build_subset.py` shows how it was made. The price lake that
`storeplan/lake.py` builds from it adds made-up trends and noise per store: those prices are illustrative.
