import matplotlib.pyplot as plt

# For Pandas
# describe
def pd_desc(df):
    return df.describe()


# mean
def mean(df):
    return df.mean()


# median
def median(df):
    return df.median()


# std
def std(df):
    return df.std()


# # visualization: bar plot
def pd_visual(df, render=True):
    df.plot(kind="hist")
    plt.xticks(rotation=90)
    plt.ylabel("Frequency")
    plt.xlabel(df.name)
    if render:
        plt.show()
    else:
        plt.savefig("histgram.png")
