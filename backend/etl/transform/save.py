import os


def save_as_csv(df, filename, folder_path):
    file_path = os.path.join(folder_path, filename)
    df.to_csv(file_path, index=False)
