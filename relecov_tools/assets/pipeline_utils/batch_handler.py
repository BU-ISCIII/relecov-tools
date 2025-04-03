import os
import pandas as pd


class BatchHandler:
    def __init__(
        self,
        j_data,
        name_key="sequencing_sample_id",
        path_key="sequence_file_path_R1_fastq",
    ):
        self.j_data = j_data
        self.name_key = name_key
        self.path_key = path_key

    def split_by_batch(self):
        """Split metadata into batches based on the folder path.

        Returns:
            dict: A dictionary {batch_path: {"j_data": list_of_samples}}.
        """
        batches = {}
        for sample in self.j_data:
            batch_dir = sample.get(self.path_key)
            if batch_dir:
                batches.setdefault(batch_dir, {"j_data": []})["j_data"].append(sample)
        return batches

    def extract_batch_rows_to_file(
        self, file, sufix, batch_samples, output_dir, header_pos, sample_colpos
    ):
        """Filter rows by sample names and write to new file with suffix.

        Args:
            file (str): Input file path.
            sufix (str): Suffix to add to the output file.
            batch_samples (list): List of sample IDs to keep.
            output_dir (str): Where to save filtered files.
            header_pos (int): Row index of header in input file.
            sample_colpos (int): Column index of the sample ID.
        """
        extdict = {".csv": ",", ".tsv": "\t", ".tab": "\t"}
        file_ext = os.path.splitext(file)[1]
        sep = extdict.get(file_ext)

        df = pd.read_csv(file, sep=sep, header=header_pos)
        df[df.columns[sample_colpos]] = df[df.columns[sample_colpos]].astype(str)
        df_filtered = df[df[df.columns[sample_colpos]].isin(batch_samples)]

        base, ext = os.path.splitext(os.path.basename(file))
        output_path = os.path.join(
            output_dir, "analysis_results", f"{base}_{sufix}{ext}"
        )
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        df_filtered.to_csv(output_path, sep=sep, index=False)
        return output_path
