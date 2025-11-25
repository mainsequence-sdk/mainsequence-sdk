
# Getting Started 4 - Markets: Equities example with Algoseek

In the previous part of the tutorial, we focused on the data layer, introducing DataNodes and how to work with them.
In this part, we’ll shift to the financial layer and explore how to integrate financial systems using the Markets platform.

## Main Sequence Markets


Main Sequence is built not only to unify data sources, but also to unify how data relates to **financial instruments**.

To accomplish this, the Main Sequence platform comes with an integrated **Markets Platform** that lets us map financial objects such as:

* Assets  
* Accounts  
* Portfolios  
* Funds  
* Trades  
* Execution Venues  
* Orders  

and many more from other financial systems.

This enables us to build robust connection layers between our existing systems and our **DataWorkflows**. This information is accessible via the [Main Sequence API](https://main-sequence.app/docs/vam-api-reference) and through the client library by importing the Main Sequence client:

![img.png](../img/tutorial/main_sequence_markets.png)


```python
import mainsequence.client as msc
```

### Assets in Main Sequence


One of the first financial objects we’ll use to build richer data workflows are **financial assets**. A common problem in data-driven finance is the lack of a universal asset master list. As finance has evolved, different service providers have built their own asset masters. While there are well-known identifiers like tickers or ISINs, these are usually a time-specific representation of a security and may not capture its full history or the requirements of a particular institution.

To address this, the Main Sequence platform makes two architectural decisions that provide both unification and flexibility to integrate any asset master and any financial system:

1. The Main Sequence platform uses a public asset master list that identifies each asset via its [FIGI](https://www.openfigi.com/).
2. The Main Sequence platform allows users to:
   a) Create custom pricing details for each public asset in the master list where a FIGI exists.  
   b) Create custom assets as part of a private master list and map them to the public master list.

   
The platform is already hydrated with information about many publicly traded assets. You can review the full list of asset attributes here:

https://github.com/mainsequence-sdk/mainsequence-sdk/blob/f3b42bd4f4478574b7375508b1b7441ca5c8d297/mainsequence/client/models_vam.py#L197

For this tutorial, focus on the most important field of an `msc.Asset` when integrating with `DataNode`s: the `unique_identifier`.

In the public master list, `unique_identifier` maps directly to the asset’s **FIGI**. In the private master list, this value is defined by the user.

It is important to note that the user is responsible for populating all attributes of any asset created in the private master list, as well as maintaining them over time.



```python
class AssetMixin(BaseObjectOrm, BasePydanticModel):
    id: Optional[int] = None

    # Immutable identifiers
    unique_identifier: constr(max_length=255)
  
```
As you saw earlier, `unique_identifier` lets you identify an asset as a **secondary index** in your tables.
For most assets this will be the FIGI. However, some assets won’t exist on https://www.openfigi.com.
In those cases, use the `is_custom_by_organization` flag to register a custom asset in the platform.

You’re not limited to classic “assets.” You can also create assets that represent **indices**,
**interest-rate curves**, or any other **unique identifier** for time‑series data.


## Example: Hydrating with AlgoAlgoSeek

FFor this example, we will integrate AlgoSeek’s daily historical bars and AlgoSeek’s Security Master List into the Main Sequence platform.

I strongly recommend that you first take a look at their data description here:
https://github.com/mainsequence-sdk/TutorialProject/tree/main/data/algoseek

### The DataNode Update Live Cycle

Now let’s get into the details of how a DataNode update cycle works. Every time we run
`data_node.run()`, the following steps occur:

1. A `StorageNode` is created if it does not already exist.
2. A `NodeUpdate` process is created if it does not already exist.
3. The `DataNode` is updated with the latest data from the `StorageNode`. This value is stored in the `DataNode` property `update_statistics: UpdateStatistics`, which contains the state of the `StorageNode`.
4. The method `get_asset_list()` is run to set the target assets in the `update_statistics` property.
5. The method `update()` is run.
6. The data is persisted in the `StorageNode`.
7. The method `get_column_metadata()` is run.
8. The method `get_table_metadata()` is run.
9. The method `run_post_update_routines()` is run.

### Markets Data Node Open Market

The Main Sequence platform also provides a data marketplace where data providers can offer and publish their data as **DataNodes**. This allows users to easily integrate provider data into their workflows without needing to build custom integrations.

As an example, we will use a highly reputable provider of enterprise-quality data, **Algoseek** (https://www.algoseek.com/).

In the data marketplace section (https://main-sequence.app/external-table-metadatas/), you already have demo data from Algoseek ready for testing your workflows. Below, we will demonstrate how we build these integrations.


### Integrating Algoseek Security Master List


The first step is to create a new DataNode that will take all the provided assets in algo seek and guarantee
that they exist in the platform if is not they case it will register them.

Create a new file in 'src/data_nodes/algo_seek_nodes.py' and copy the content
from
https://github.com/mainsequence-sdk/TutorialProject/blob/main/src/data_nodes/algo_seek_nodes.py





#### get_asset_list method

In most real-time applications, you will build DataNodes that get their assets dynamically or from
a predefined filter—for example, all the assets in the S&P 500, all the assets in your benchmark,
or, in this case, all the assets in the Algoseek Security Master List.

Here is the diagram of the cycle:

--8<-- "_includes/datanode_cycle.html"


```python
 def get_asset_list(self) -> List[msc.Asset]:
        mapping = _load_security_master_mapping_figi_rows(self.csv_path)
        figis = mapping["unique_identifier"].tolist()
        self.mapping=mapping
        return _get_or_register_assets_from_figis(figis)

```

the private function `_load_security_master_mapping_figi_rows` basically just loads the csv file you can check it the source
what we are interested is in how we are registering the assets in the platform.

```python
def _get_or_register_assets_from_figis(figis: List[str], batch_size: int = 500) -> List[msc.Asset]:
    """
    1) Find existing via Asset.filter(unique_identifier__in=...)
    2) Register missing via Asset.register_figi_as_asset_in_main_sequence_venue(figi)
    Returns msc.Asset objects (existing + newly registered).
    """
    figis = sorted(set([f for f in figis if f]))


    # Find existing in batches (avoid unknown per_page arg)

    existing_assets = msc.Asset.query(unique_identifier__in=figis, per_page=batch_size)
    existing_figis = {a.unique_identifier: a for a in existing_assets}

    # Register missing
    missing = [f for f in figis if f not in existing_figis]
    newly_registered: List[msc.Asset] = []
    for figi in missing:
        try:
            a = msc.Asset.register_asset_from_figi(figi=figi)
            newly_registered.append(a)

        except Exception as e:
            # Asset may already exist or FIGI invalid – continue with others.
            raise e

    return existing_assets + newly_registered

```

Let’s look at the important lines.

First, we filter the assets that already exist in the platform via their unique identifier, such as the FIGI.
Here we are using the `msc.Asset.query()` method to query the platform assets. It is recommended to use a batch size of about 500
when pulling thousands of assets from the platform.
```python
 existing_assets = msc.Asset.query(unique_identifier__in=figis, per_page=batch_size)
```

Now we loop through all the FIGIs that are not yet registered in the platform, and we run:
```python
msc.Asset.register_asset_from_figi(figi=figi)
```

This will automatically register the asset in the public master list.

#### get_column_metadata method

The `get_column_metadata` method is a utility method that helps us add metadata to our columns. This is very useful
when sharing our data with other users as well as when integrating the data into agentic workflows.

The method is quite simple: it expects a `List[ColumnMetaData]`, where each element is a `ColumnMetaData` object.
Not all columns are required to have metadata, but it is good practice to add metadata to all columns. Be sure that the
`column_name` matches exactly the column name in the dataframe you are providing.

**Important Note:** If it is not clear by now, once the DataStorage has been created, the process that generates the data
must always provide the same columns. If this is not the case, the update process will fail.


#### get_table_metadata method

The `get_table_metadata` method is a utility method that helps us add metadata to our tables. This is very handy
when sharing data with other users as well as when integrating the data into agentic workflows. 

The most important attribute is the `identifier`. This is a unique, human-readable identifier that can be used to access
the data without needing access to the generating code through our unified API.


#### update method

We have already explored the update method. However, in this particular case, the Asset Master List is not really time-series data.
We could have kept this node as just a process from a file, but we built it as a DataNode to demonstrate how we can force a process update.

For this reason, in this case we will always overwrite the values, assuming that the source file of the asset master list is updated or maintained
by an external process.

### Integrating Algoseek Daily Historical Bars

Now, to integrate the daily historical bars, we will first define the constructor with our dependency on the Security Master List node.

```python
def __init__(self, daily_dir: str, security_master_path, is_demo: bool, *args, **kwargs):
        self.daily_dir = daily_dir
        self.security_master_node = AlgoSeekSecMasDEM(csv_path=security_master_path,is_demo=is_demo)
        self.is_demo = is_demo
        super().__init__(*args, **kwargs)

def dependencies(self) -> Dict[str, "DataNode"]:
    # Ensure master runs first
    return {"security_master_node": self.security_master_node}
```


The `get_table_metadata` method and the `get_column_metadata` method are very similar to the ones we saw in the previous example,
so we will not go into detail here. However, let’s look at the `get_asset_list` method.


```python
    # ---- Asset universe (SecId -> FIGIs) ----
    def get_asset_list(self) -> List[msc.Asset]:
        master_list = self.security_master_node.get_df_between_dates()
        all_sec_ids = []
        for df_path in self._iter_minute_gz_files():
            secid =self._secid_from_path(df_path)
            all_sec_ids.append(secid)

        # this will repeat all the figis with the same secid so we have duplicated data per figi, this is better as we can query per figi the full story taht corresponds to the sec id
        available_assets = master_list[master_list.secid.isin(all_sec_ids)].index.get_level_values(
            "unique_identifier").to_list()
        self.security_master_df = master_list
        asset_list = _get_or_register_assets_from_figis(figis=available_assets)

        self.sec_id_to_figi_map = (
            self.security_master_df
            .reset_index()
            .groupby("secid", sort=False)["unique_identifier"]
            .apply(lambda s: list(dict.fromkeys(s.dropna())))
            .to_dict()
        )

        return asset_list
```

The first interesting part is using `self.security_master_node.get_df_between_dates()`. This will dump the full
table in the backend. In this case, since we are just testing, it is fine to make a full dump. However, this is not recommended
for real applications. Ideally, we would apply filters using a combination of the parameters in this method:


```python
def get_df_between_dates(
        self,
        start_date: datetime.datetime | None = None,
        end_date: datetime.datetime | None = None,
        unique_identifier_list: list | None = None,
        great_or_equal: bool = True,
        less_or_equal: bool = True,
        unique_identifier_range_map: UniqueIdentifierRangeMap | None = None,
        columns: list[str] | None = None,
    ) -> pd.DataFrame:
```
**IMPORTANT** 
In most cases, we will actually use a helper pattern. In this case, range_descriptor gives us a filter for each asset
in update_statistics, requesting data great_or_equal for that specific asset:

for any data_node:
```python
range_descriptor = update_statistics.get_update_range_map_great_or_equal()
last_observation = self.get_ranged_data_per_asset(range_descriptor=range_descriptor)
```

After this, we apply a few filters and build a map between the Algoseek sec_id and the FIGI. This is essentially the mapping key used to integrate the Algoseek master list with the Main Sequence public master list.


```python
 self.sec_id_to_figi_map = (
            self.security_master_df
            .reset_index()
            .groupby("secid", sort=False)["unique_identifier"]
            .apply(lambda s: list(dict.fromkeys(s.dropna())))
            .to_dict()
        )
 ```

And we are done. The update method follows a simple logic to extract the data from a CSV and format it in the way the platform expects.


Now to test your DataNode build a launcher and run it as we have done in the previous examples.