import pandas as pd
import click
import glob
import os
import numpy as np
import functools


@click.command()
@click.argument("results_dir", help="Path to folder (local or S3) containing geotrellis results.")
def get_qc_results(results_dir):
    analysis_results = os.listdir(results_dir)
    analysis_dfs = []

    for result_path in analysis_results[1:]:
        result_type = get_result_type(result_path)
        if result_type == "tcl":
            part_files = glob.glob(f"{results_dir}/{result_path}/adm2/change/*.csv")
        else:
            part_files = glob.glob(f"{results_dir}/{result_path}/adm2/daily_alerts/*.csv")

        part_dfs = [pd.read_csv(file, sep='\t') for file in part_files]
        result_df = pd.concat(part_dfs)
        adm_cols = ["iso", "adm1", "adm2"]

        row_dfs = []
        for row in qc_config(result_type):
            contextual_filters = [
                result_df[col] == True if "is__" in col else result_df[col].notnull()
                for col in row["layers"]
            ]
            adm_filters = [result_df[col] == row[col] for col in adm_cols]

            if result_type == "glad":
                date_filters = [result_df["alert__date"] >= "2020-06-01", result_df["alert__date"] < "2020-09-01"]
                filters = contextual_filters + adm_filters + date_filters
                row_df = get_agg_df(result_df, adm_cols, ["alert__count"], filters)
                row_df = row_df.rename(columns={"alert__count": result_type})
            elif result_type == "viirs" or result_type == "modis":
                date_filters = [result_df["alert__date"] >= "2020-06-01", result_df["alert__date"] < "2020-09-01"]
                filters = adm_filters + date_filters
                row_df = get_agg_df(result_df, adm_cols, ["alert__count"], filters)
                row_df = row_df.rename(columns={"alert__count": result_type})
            elif result_type == "tcl":
                groupby_fields = adm_cols + ["umd_tree_cover_loss__year"]
                filters = contextual_filters + adm_filters + [result_df["umd_tree_cover_density__threshold"] == 30]
                row_df = get_agg_df(result_df, groupby_fields, ["umd_tree_cover_loss__ha"], filters)
                row_df = row_df.pivot(index=adm_cols, columns='umd_tree_cover_loss__year',
                                      values='umd_tree_cover_loss__ha').reset_index()

            row_dfs.append(row_df)

        analysis_df = pd.concat(row_dfs)
        analysis_dfs.append(analysis_df)

    final_df = functools.reduce(lambda left, right: pd.merge(left, right, how='outer', on=['iso', 'adm1', 'adm2']), analysis_dfs)
    final_df.to_csv(f"{results_dir}/qc_results.csv", index=False)

    # qc_df = pd.read_csv(qc_path).set_index(['Contextual Layers', 'NAME_0', 'NAME_1', 'NAME_2']).reset_index(drop=True)
    # final_df = final_df.set_index(adm_cols).reset_index(drop=True)
    #
    # error_ratio_df = (np.abs(qc_df - final_df) / qc_df).replace([np.nan, np.inf], 0) * 100
    # error_ratio_df.to_csv(f"{results_dir}/error_ratios.csv", index=False)


def get_result_type(result_path):
    if "annualupdate" in result_path:
        return "tcl"
    elif "gladalerts" in result_path:
         return "glad"
    elif "viirs" in result_path:
         return "viirs"
    elif "modis" in result_path:
         return "modis"

def get_agg_df(df, groupby_fields, sum_fields, filters):
    return df[functools.reduce(np.logical_and, filters)][(groupby_fields + sum_fields)].groupby(groupby_fields).sum().reset_index()


def qc_config(result_type):
    ifl_col = "ifl_intact_forest_landscape__year" if result_type == "tcl" else "is__ifl_intact_forest_landscape_2016"
    tiger_col = ["is__gfw_tiger_landscape"] if result_type == "tcl" else []

    return [
        {
            "iso": "BRA",
            "adm1": 10,
            "adm2": 56,
            "layers": ["is__birdlife_key_biodiversity_area", "is__birdlife_alliance_for_zero_extinction_site", "is__landmark_land_right", "is__gfw_mining"]
        },
        {
            "iso": "CUB",
            "adm1": 16,
            "adm2": 5,
            "layers": ["is__birdlife_alliance_for_zero_extinction_site", "is__umd_regional_primary_forest_2001", "is__gmw_mangroves_2016"]
        },
        {
            "iso": "MYS",
            "adm1": 14,
            "adm2": 10,
            "layers": ["is__gfw_wood_fiber", "wdpa_protected_area__iucn_cat"]
        },
        {
            "iso": "IDN",
            "adm1": 12,
            "adm2": 2,
            "layers": ["is__idn_forest_moratorium", "is__gfw_oil_palm"]
        },
        {
            "iso": "BRA",
            "adm1": 14,
            "adm2": 77,
            "layers": ["is__birdlife_key_biodiversity_area", "is__birdlife_alliance_for_zero_extinction_site", "is__landmark_land_right", "is__gfw_mining"]
        },
        {
            "iso": "IDN",
            "adm1": 24,
            "adm2": 9,
            "layers": [ifl_col, "gfw_plantation__type"] + tiger_col
        },
        {
            "iso": "KHM",
            "adm1": 22,
            "adm2": 3,
            "layers": ["is__birdlife_key_biodiversity_area", "is__birdlife_alliance_for_zero_extinction_site", "is__landmark_land_right", "is__gfw_mining"]
        },
        {
            "iso": "IDN",
            "adm1": 12,
            "adm2": 14,
            "layers": ["is__idn_forest_moratorium", "is__gfw_oil_palm"]
        },
        {
            "iso": "IDN",
            "adm1": 30,
            "adm2": 18,
            "layers": [ifl_col, "gfw_plantation__type"] + tiger_col
        },
        {
            "iso": "KHM",
            "adm1": 20,
            "adm2": 8,
            "layers": ["is__birdlife_key_biodiversity_area", "is__birdlife_alliance_for_zero_extinction_site",
                       "is__landmark_land_right", "is__gfw_mining"]
        },
        {
            "iso": "MYS",
            "adm1": 14,
            "adm2": 31,
            "layers": ["is__gfw_wood_fiber", "wdpa_protected_area__iucn_cat"]
        },
        {
            "iso": "KHM",
            "adm1": 20,
            "adm2": 9,
            "layers": ["is__birdlife_key_biodiversity_area", "is__birdlife_alliance_for_zero_extinction_site",
                       "is__landmark_land_right", "is__gfw_mining"]
        },
    ]


if __name__ == "__main__":
    get_qc_results()

#
# "BRA","Brazil",10,"Maranhão",56,"Centro Novo do Maranhão",
# "CUB","Cuba",16,"Villa Clara",5,"Encrucijada"
# "MYS","Malaysia",14,"Sarawak",10,"Kapit"
# "IDN","Indonesia",12,"Kalimantan Barat",2,"Kapuas Hulu"
# "BRA","Brazil",14,"Pará",77,"Nova Esperança do Piriá"
# "IDN","Indonesia",24,"Riau",9,"Pelalawan"
# "KHM","Cambodia",22,"Stœng Trêng",3,"Siem Pang"
# "IDN","Indonesia",12,"Kalimantan Barat",14,"Sintang"
# "IDN","Indonesia",30,"Sumatera Barat",18,"Solok Selatan"
# "KHM","Cambodia",20,"Rôtânôkiri",8,"Ta Veaeng"
# "MYS","Malaysia",14,"Sarawak",31,"Tatau"
# "KHM","Cambodia",20,"Rôtânôkiri",9,"Veun Sai"