# -*- coding: utf-8 -*-
"""
Created on Fri Feb 7 15:13:47 2025
Revised on Tue Mar 11 12:05:26 2025

Run Data Query via Faceset Earnings Reports API (CDietz)


@author: TQiu
"""


import datetime
import pandas as pd
import requests


def _get_json_raw_report(report_id="2973474"):
    """returns a json from api"""
    _api = "http://ace/prd/imt/nlptranscriptscontroller/v2/otf/read/report/text?"
    _api += "reportId="
    _api += report_id
    _api += "&appName=llmRC"
    return requests.get(_api).json()


def _parse_json_raw_report(rpt_json):
    """returns a df of securities, a df of reports, a df of parents, a df of batch summary"""
    _all_spkrs = set()
    _com_spkrs = set()
    _mgmt_disc = []
    _quest_ans = []
    _prev_sctn = -99
    _prev_clst = -99
    _prev_spkr = -99
    _prev_list = None
    _prev_text = ""
    _curr_sctn = -99
    _curr_clst = -99
    _curr_spkr = -99
    _curr_list = None
    _curr_text = ""
    _tmp_para = None
    for _para in rpt_json["reportContent"]:
        if _para["paraId"] > 0:
            _curr_text += _para["paraText"]+"\n"
            continue
        
        _curr_sctn = _para["sectionId"]
        _curr_clst = _para["textClusterId"]
        _curr_spkr = _para["speakerId"]
        _curr_list = _para["paraListType"]
        if _curr_spkr != _prev_spkr:
            _all_spkrs.add(_curr_spkr)
            if _curr_sctn == 0:
                _com_spkrs.add(_curr_spkr)
        
        if _curr_sctn != _prev_sctn or len(_curr_text.split()) > 100 and ( \
            (_curr_sctn == 0 and _prev_spkr != 0 and _curr_spkr != _prev_spkr) or \
            (_curr_sctn >= 1 and _curr_spkr == 0 and _curr_spkr != _prev_spkr)):
            if _tmp_para is not None:
                _tmp_para.update({"paragraph" : _curr_text})
                if _prev_sctn == 0:
                    _mgmt_disc.append(_tmp_para)
                if _prev_sctn == 1:
                    _quest_ans.append(_tmp_para)
            _tmp_para = {}
            _tmp_para.update({"section" : _curr_sctn})
            _curr_text = "#### "+_para["sectionName"]+" ####\n"
        
        if _curr_sctn >= 1:
            if _curr_spkr != _prev_spkr:
                if _curr_list is not None:
                    if _curr_list == 'q':
                        _curr_text += ">>> Question: >>>\n"
                    if _curr_list == 'a':
                        _curr_text += "<<< Answer: <<<\n"
        
        if _curr_spkr != _prev_spkr:
            _curr_text += "[speaker_"+str(_curr_spkr)+"]\n"
        
        _curr_text += _para["paraText"]+"\n"
                
        _prev_sctn = _curr_sctn
        _prev_clst = _curr_clst
        _prev_spkr = _curr_spkr
        _prev_list = _curr_list
        _prev_text = _para["paraText"]+"\n"
    
    _curr_text += "\n<<END OF TRANSCRIPTS>>\n\n"
    _tmp_para.update({"paragraph" : _curr_text})
    _quest_ans.append(_tmp_para)
    return pd.DataFrame(_mgmt_disc), pd.DataFrame(_quest_ans), pd.Series(list(_all_spkrs), name="SpeakerId"), pd.Series(list(_com_spkrs), name="SpeakerId")


def _get_json_reports(entity_id="590652", start_date="01/01/2024", end_date="01/01/2025"):
    """returns a json from api"""
    _api = "http://ace/dev/arc/fsenrich/api/getEnrichedPortfolioSecurities?"
    _api += "entityId="
    _api += entity_id
    _api += "&startDate="
    _api += start_date
    _api += "&endDate="
    _api += end_date
    _api += "&addSavedBatchScores=Y"
    return requests.get(_api).json()


def _parse_json_reports(reports_json):
    """returns a df of securities, a df of reports, a df of parents, a df of batch summary"""
    _summary = reports_json["batchHistogram"]
    _parents = reports_json["parentEntities"]
    _sec_data = []
    _reports = []
    for _sec in reports_json["securities"]:
        
        if len(_sec["issuerReports"]) == 0:
            pass #could process bad reports here
        else:
            _sid = _sec['securityId']
            _tmp = {}
            for _key in _sec.keys():
                if _key == "issuerReports":
                    _rpts = _sec[_key]
                elif _key == "fsQtrFundamentals":
                    pass #currently empty list, revisit later
                else:
                    _tmp.update({_key : _sec[_key]})
            _sec_data.append(_tmp)
            for _rpt in _rpts:
                if _rpt["batchAnalysis"] is None:
                    pass #could process bad reports here
                else:
                    _tmp = {}
                    _tmp.update({"securityId" : _sid})
                    for _key in _rpt.keys():
                        if _key == "batchAnalysis":
                            for _scr in _rpt[_key].keys():
                                _tmp.update({_scr : _rpt[_key][_scr]["score"], 
                                            _scr+"_comment" : _rpt[_key][_scr]["comment"]})
                        else:
                            _tmp.update({_key : _rpt[_key]})
                    _reports.append(_tmp)
    return pd.DataFrame(_sec_data), pd.DataFrame(_reports), pd.DataFrame(_parents), pd.DataFrame([_summary])


def get_reports(entity_id="590652", start_date="01/01/2024", end_date="01/01/2025", folder_path="."):
    print("...downloading earnings report......")
    df_sec, df_rpt, df_prt, df_bch = _parse_json_reports(_get_json_reports(entity_id, start_date, end_date))
    str_start = datetime.datetime.strptime(start_date, "%m/%d/%Y").strftime("%Y%m%d")
    str_end = datetime.datetime.strptime(end_date, "%m/%d/%Y").strftime("%Y%m%d")
    str_fname = folder_path + "/data_security_" + entity_id + str_start + str_end + ".csv"
    df_sec.to_csv(str_fname)
    str_fname = folder_path + "/data_reports_" + entity_id + str_start + str_end + ".csv"
    df_rpt.to_csv(str_fname)
    str_fname = folder_path + "/data_parents_" + entity_id + str_start + str_end + ".csv"
    df_prt.to_csv(str_fname)
    str_fname = folder_path + "/data_batch_" + entity_id + str_start + str_end + ".csv"
    df_bch.to_csv(str_fname)
    return


def load_reports(entity_id="590652", start_date="01/01/2024", end_date="01/01/2025", folder_path="."):
    str_start = datetime.datetime.strptime(start_date, "%m/%d/%Y").strftime("%Y%m%d")
    str_end = datetime.datetime.strptime(end_date, "%m/%d/%Y").strftime("%Y%m%d")
    str_fname = folder_path + "/data_security_" + entity_id + str_start + str_end + ".csv"
    df_sec = pd.read_csv(str_fname, index_col=0)
    str_fname = folder_path + "/data_reports_" + entity_id + str_start + str_end + ".csv"
    df_rpt = pd.read_csv(str_fname, index_col=0)
    print("loaded all securities and their earnings reports.")
    return df_sec, df_rpt


def read_report(report_id="2973474"):
    return _parse_json_raw_report(_get_json_raw_report(report_id))
    

__all__ = ["get_reports", "load_reports", "read_report"]

