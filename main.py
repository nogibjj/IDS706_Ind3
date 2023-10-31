from lib import pd_desc, pd_visual
import pandas as pd


def summary_desc():
    df = pd.read_csv("cereal.csv")
    # return [mean(df), median(df), std(df)]
    return pd_desc(df)


def visual():
    df = pd.read_csv("cereal.csv")
    pd_visual(df["calories"])


def save_to_md():
    df = pd.read_csv("cereal.csv")
    tab1 = pd_desc(df).to_markdown()
    pd_visual(df["calories"], render=False)
    with open("summary.md", "w", encoding="utf-8") as file:
        file.write("Describe:\n")
        file.write(tab1)
        file.write("\n\n")  # Add a new line
        file.write("![Histgram](histgram.png)\n")
