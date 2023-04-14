import tkinter as tk
from tkinter import HORIZONTAL, Scrollbar
from tkinter import ttk
from tkinter import filedialog as fd
from tkinter import NONE
from tkinter import *
from tkinter import messagebox
import json
import pandas as pd



class App:
    def __init__(self, master, **kwargs):
        self.master = master
        self.create_json_text_display()
    def create_json_text_display(self):
        self.label = tk.Label(root, height=1, text="Datamesh Pipeline Metadata", padx=25)
        self.label.grid(column=1, row=0, sticky=N+E+S+W, columnspan=2)
        self.textbox = tk.Text(self.master, height = 20, width = 79, wrap = NONE)
        vertscroll = Scrollbar(self.master)
        horizscroll = Scrollbar(self.master, orient=HORIZONTAL)
        vertscroll.config(command=self.textbox.yview)
        horizscroll.config(command=self.textbox.xview)
        self.textbox.config(xscrollcommand=horizscroll.set, yscrollcommand=vertscroll.set)
        self.textbox.grid(column=0, row=1, padx=25, columnspan=4, sticky=N+E+S+W)
        vertscroll.grid(column=4, row=1, sticky='NS')
        horizscroll.grid(column=0, row=2, sticky='EW', columnspan=4)
        self.selected_excel_file = tk.Label(root, height=1, text="No Excel file currently selected", padx=25)
        self.selected_excel_file.grid(column=0, row=3, sticky='w', columnspan=3)
        self.textbox.update()
        self.create_file_selection_button()
        self.create_view_dq_rules_button()
        self.create_view_data_cln_button()
        self.create_view_pipeline_metadata_button()

    def create_view_data_cln_button(self):
        # open file button
        open_button = ttk.Button(
            root,
            text='View Data Classification JSON',
            command=self.display_data_cln_json)
        open_button.grid(column=3, row=4, sticky='NSEW', padx=25, pady=15)
    

    def create_view_dq_rules_button(self):
        # open file button
        open_button = ttk.Button(
            root,
            text='View DQ Rule Sets JSON',
            command=self.display_dq_rules_json)
        open_button.grid(column=2, row=4, sticky='NSEW', padx=25, pady=15)


    def create_view_pipeline_metadata_button(self):
        # open file button
        open_button = ttk.Button(
            root,
            text='View Pipeline Metadata JSON',
            command=self.display_pipeline_metadata_json)
        open_button.grid(column=1, row=4, sticky='NSEW', padx=25, pady=15)

    #@logger_util.common_logger_exception
    def create_file_selection_button(self):
        # open file button
        open_button = ttk.Button(
            root,
            text='Select an Excel File',
            command=self.open_excel_file)
        open_button.grid(column=0, row=4, sticky='w', padx=25, pady=15)


    def open_excel_file(self):
        # file type
        filetypes = (
            ('excel file', '*.xlsx'),
        )
        # show the open file dialog
        try:
            f = fd.askopenfile(filetypes=filetypes)
        except Exception as e:
            error_string = f"The Excel file was not accessible. Please ensure that the Excel file is closed and retry. \n\n{str(e)}"
            messagebox.showerror("error", error_string)
            return
        try:
            filename = f.name
        except Exception as e:
            error_string = f"No Excel file was selected for JSON conversion. Please select an Excel file to proceed."
            messagebox.showerror("error", error_string)
            return

        # convert excel to json, and display contents
        self.metadata_json, self.dq_rules_json, self.data_cln_json = self.get_json_from_excel_sheets(filename)
        pipeline_meadata_json = json.dumps(self.metadata_json, indent=2)
        try:
            fi_name = filename.split(".")[0].split("/")[-1]
            out_file = open(f"{fi_name}.json", "w")
            json.dump(self.metadata_json, out_file, indent=2)
            out_file.close()

            out_file = open(f"{fi_name}_dq_rules.json", "w")
            json.dump(self.dq_rules_json, out_file, indent=2)
            out_file.close()

            out_file = open(f"{fi_name}_data_classification.json", "w")
            json.dump(self.data_cln_json, out_file, indent=2)
            out_file.close()

            self.textbox.delete("1.0", "end")
            self.textbox.insert('1.0', pipeline_meadata_json)
            self.selected_excel_file.config(text="Selected Excel File: " + filename)
        except Exception as e:
            error_string = f"An error occurred while writing to individual JSON files. See below for the error: \n\n{str(e)}"
            messagebox.showerror("error", error_string)
            return


    def get_json_from_excel_sheets(self,filename):
        try:
            # build json from excel sheet data
            metadata_json = dict()
            dq_rules_json = dict()
            data_cln_json = dict()
            sheet_dfs = pd.read_excel(filename, engine='openpyxl', sheet_name=None)
            for sheet, data in sheet_dfs.items():
                data = data.fillna('')
                data = data.drop(data.loc[:,data.columns[data.columns.str.startswith("Unnamed:")]], axis=1)
                if sheet == "DataMeshMetadataTable":
                    # Build initial json schema
                    initial_json = data.to_dict(orient="records")
                    for key, value in initial_json[0].items():
                        if key == "target_file_name":
                            target_file_name = value
                        if key == "target_table_name":
                            target_table_name = value
                        if key == "target_type":
                            target_type = value
                        if value != '':
                            if key == "cdc_key_set":
                                key = "cdc_timestamp_key"
                            if key == "delta_key_set":
                                key = "log_chng_indicator_key"
                            if key != "target_name":
                                metadata_json[key] = value
                            if key == "app_id" or key == "version_number":
                                metadata_json[key] = int(value)
                            if key.endswith("key_set") or key == "cdc_timestamp_key" or key == "log_chng_indicator_key":
                                if value == "":
                                    metadata_json[key] = []
                                else:
                                    value = value.split(",")
                                    value = [item.strip() for item in value]
                                    metadata_json[key] = value
                            elif key == "load_frequency":
                                metadata_json["source_environments"] = []
                            elif key == "target_name":
                                metadata_json["target_environments"] = []
                                metadata_json["pipeline_columns"] = []
                    if target_type == "s3":
                        metadata_json['target_file_name'] = target_file_name
                    else:
                        metadata_json['target_table_name'] = target_table_name
                elif sheet == "DataMeshMetadataEnv":
                    # Populate source and target env data
                    env_data = data.to_dict(orient="records")
                    source_environments, target_environments = self.get_source_target_env_dicts(env_data)
                    metadata_json["source_environments"] = source_environments
                    metadata_json["target_environments"] = target_environments
                elif sheet == "DataMeshMetadataColumn":
                    new_meta_data = []
                    meta_data = data.to_dict(orient="records")
                    for item in meta_data:
                        new_dict = {}
                        for key, value in item.items():
                            if value != '':
                                if 'precision' in key or 'length' in key or 'sequence_number' in key:
                                    new_dict[key] = int(value)
                                else:
                                    new_dict[key] = value
                        new_meta_data.append(new_dict)
                    #new_meta_data_list = sorted(new_meta_data, key=lambda d: d['sequence_number'])
                    metadata_json["pipeline_columns"] = new_meta_data
                elif sheet == "DataMeshDQRules":
                    dq_rules_data = data.to_dict(orient="records")
                    dq_rules_json["pipeline_id"] = "<Enter pipeline id here>"
                    dq_rules_json["version_number"] = metadata_json["version_number"]
                    dq_rules_json["pipeline_columns"] = []
                    for record in dq_rules_data:
                        if record["source_column_name"] == "":
                            break
                        src_col_name = record["source_column_name"]
                        dq_rule = record["dq_rule"]
                        dq_desc = record["dq_desc"]
                        if not any(src_col_name in d.values() for d in dq_rules_json["pipeline_columns"]):
                            dq_rules_json["pipeline_columns"].append({"source_column_name":src_col_name, \
                                "dq_rule_set":[{"dq_rule":dq_rule, "dq_desc":dq_desc}]})
                        else:
                            del record["sequence_number"]
                            del record["source_column_name"]
                            index = [i for i, d in enumerate(dq_rules_json["pipeline_columns"]) if "source_column_name" in d.keys()][0]
                            dq_rules_json["pipeline_columns"][index]["dq_rule_set"].append(record)
                elif sheet == "DataMeshClassification":
                    data_cln = data.to_dict(orient="records")
                    data_cln_json["pipeline_id"] = "<Enter pipeline id here>"
                    data_cln_json["version_number"] = metadata_json["version_number"]
                    for record in data_cln:
                        if record["sequence_number"] == "":
                            data_cln.remove(record)
                    data_cln_json["data_catalog_columns"] = data_cln
        except Exception as e:
            error_string = f"An error occurred while converting the excel sheet contents to JSON. See below for the error: \n\n{str(e)}"
            messagebox.showerror("error", error_string)
        return metadata_json, dq_rules_json, data_cln_json


    def get_source_target_env_dicts(self, env_data):
        source_environments = []
        target_environments = []
        source_env = dict()
        target_env = dict()
        for env_dict in env_data:
            for k, v in env_dict.items():
                if v != '':
                    prefix = k.split("_")[0]
                    if "source" == prefix:
                        if k == "source_bucket_name":
                            source_env[k] = v
                        else:
                            if k != "database_schema_name":
                                k = "_".join(k.split("_")[1:])
                                source_env[k] = v
                    elif "target" == prefix:
                        if k == "target_bucket_name" or k == "target_table_retention_days":
                            target_env[k] = v
                        else:
                            k = "_".join(k.split("_")[1:])
                            target_env[k] = v
                    else:
                        target_env[k] = v
            source_environments.append(source_env)
            target_environments.append(target_env)
            source_env = dict()
            target_env = dict()
        return source_environments, target_environments



    def display_pipeline_metadata_json(self):
        try:
            self.metadata_json
        except Exception as e:
            error_string = f"Please select an Excel file before viewing pipeline metadata JSON"
            messagebox.showerror("error", error_string)
            return
        try:
            self.textbox.delete("1.0", "end")
            self.textbox.insert('1.0', json.dumps(self.metadata_json, indent=2))
        except Exception as e:
            error_string = f"An error occurred while inserting text for pipeline metadata. See below for the error: \n\n{str(e)}"
            messagebox.showerror("error", error_string)


    def display_dq_rules_json(self):
        try:
            self.dq_rules_json
        except Exception as e:
            error_string = f"Please select an Excel file before viewing DQ rule sets JSON"
            messagebox.showerror("error", error_string)
            return
        try:
            self.textbox.delete("1.0", "end")
            self.textbox.insert('1.0', json.dumps(self.dq_rules_json, indent=2))
        except Exception as e:
            error_string = f"An error occurred while inserting text for dq rule sets. See below for the error: \n\n{str(e)}"
            messagebox.showerror("error", error_string)
    

    def display_data_cln_json(self):
        try:
            self.data_cln_json
        except Exception as e:
            error_string = f"Please select an Excel file before viewing data classification JSON"
            messagebox.showerror("error", error_string)
            return
        try:
            self.textbox.delete("1.0", "end")
            self.textbox.insert('1.0', json.dumps(self.data_cln_json, indent=2))
        except Exception as e:
            error_string = f"An error occurred while inserting text for data classification. See below for the error: \n\n{str(e)}"
            messagebox.showerror("error", error_string)


    def resize(self, event_handler):
        try:
            pixelX=root.winfo_width() - 40
            pixelY=root.winfo_height() - 20
            self.textbox["width"]=int(pixelX/8.25) 
            self.textbox["height"]=int(pixelY/20)
        except Exception as e:
            error_string = f"An error occurred while attempting to render the Excel to JSON converter application. See below for the error: \n\n{str(e)} \
                \n\nShutting down application...."
            messagebox.showerror("error", error_string)
            root.destroy()


if __name__ == '__main__':
    root = tk.Tk()
    root.title("Excel to JSON Converter")
    app = App(root)
    root.bind("<Configure>", app.resize)
    root.mainloop()