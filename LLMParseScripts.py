# -*- coding: utf-8 -*-
"""
Created on Fri Mar 28 15:07:02 2025

Read Earnings Transcripts via LLM


@author: TQiu
"""


import os
import json
import pandas as pd
from utils_gpt import run_gpt_azure
from FSReportApiData import read_report


def get_scores(df_md, df_qa, _tmp_qa):
    os.environ['REQUESTS_CA_BUNDLE'] = "C:/Program Files/Microsoft SDKs/Azure/CLI2/Lib/site-packages/certifi/cacert.pem"
    
    prompt_template = "{input}\n Question: Only use information in the transcripts above, {your_question} Answer: "
    sys_prompt_an = "You are a successful senior credit research analyst of a leading hedge fund, and you have been following this company for a decade and knowing all the nuances in its corresponding sector. You are now preparing your research recommendation for the portfolio manager regarding this latest earnings call."
    sys_prompt_ex = "You are a successful senior executive of this company. Your colleagues just presented the prepared remarks, and you are tasked to answer the investors' questions. You like to be direct and transparent which would help to gain the confidence and trust."

    _context = _tmp_qa
    _prompt = prompt_template.replace("{input}", _context)
    _usr_prompt_an = "Rephrase the questions asked by the speaker from casual verbal language to unambiguous professional written language, need to make sure the tone of the questions remain the same as the original questions from this speaker, and also correctly interpreting the intention of raising these questions to the management team:"
    _prompt = _prompt.replace("{your_question}", _usr_prompt_an)
    _rephrs_q = run_gpt_azure(_prompt, False, False, 'gpt-4o', sys_prompt_an)

    _context = "\n".join(df_md.paragraph.tolist())
    _prompt = prompt_template.replace("{input}", _context)
    _usr_prompt_ex = "Only using the prepared remarks above, try your best to answer the following questions from the hedge fund research analyst. Make a note of which speaker's remarks you used for the response. If the prepared remarks above cannot address the questions, simply reply 'Not mentioned.' without elaborating. Here's the questions asked:\n"
    _usr_prompt_ex += _rephrs_q
    _prompt = _prompt.replace("{your_question}", _usr_prompt_ex)
    _rephrs_a = run_gpt_azure(_prompt, False, False, 'gpt-4o', sys_prompt_ex)

    _context = _rephrs_q+"\n"+_rephrs_a+"\n".join(df_md.paragraph.tolist())
    _prompt = prompt_template.replace("{input}", _context)
    _usr_prompt_an = "Review the prepared remarks from management, give a score of Proactiveness to show how well the management team's prepared remarks proactively addressed the questions raised by the analyst (score range 1-10, 10 being fully prepared, 1 being caught unprepared). Then if the score is less than 10, briefly comment on why to deduct points. Reply in json format with fields: score (integer), comment (string)."
    _prompt = _prompt.replace("{your_question}", _usr_prompt_an)
    _response_an1 = run_gpt_azure(_prompt, False, False, 'gpt-4o', sys_prompt_an)
    _score1 = json.loads(_response_an1)
    _score1.update({"type" : "proactiveness"})

    _context = _tmp_qa
    _prompt = prompt_template.replace("{input}", _context)
    _usr_prompt_an = "Now reviewing the live Q&A dialogue above, give a score of Relevance to show how well the Management Team's answers effectively addressed the questions raised by the analyst (score range 1-10, 10 being staying on-topic and fully addressed the questions, 1 being going off-topic and leaving some part of questions unaddressed). Then if the score is less than 10, briefly comment on why to deduct points. Reply in json format with fields: score (integer), comment (string)."
    _prompt = _prompt.replace("{your_question}", _usr_prompt_an)
    _response_an2 = run_gpt_azure(_prompt, False, False, 'gpt-4o', sys_prompt_an)
    _score2 = json.loads(_response_an2)
    _score2.update({"type" : "relevance"})

    _context = df_qa.paragraph[0]+"\n\n\n###### Synthesized Q&A ######\n"+_rephrs_q+"\n"+_rephrs_a
    _prompt = prompt_template.replace("{input}", _context)
    _usr_prompt_an = "Now comparing the live Q&A dialogue above, vs the synthesized Q&A based on the prepared remarks, give a score of Transparency to show how much the management team is willing to disclose more information than those already discussed in the prepared remarks, and how much they are being transparent of what they really believe of the company regarding the analyst's questions (score range 1-10, 10 being fully transparent and speaking the truth and everything they know about the company, 1 being ambiguous and using media training techniques to avoid revealing their real thoughts). Then if the score is less than 10, briefly comment on why to deduct points. Reply in json format with fields: score (integer), comment (string)."
    _prompt = _prompt.replace("{your_question}", _usr_prompt_an)
    _response_an3 = run_gpt_azure(_prompt, False, False, 'gpt-4o', sys_prompt_an)
    _score3 = json.loads(_response_an3)
    _score3.update({"type" : "transparency"})

    return [_score1, _score2, _score3]



def digest_report(report_id=2973474):
    df_md, df_qa, df_sp_all, df_sp_com = read_report(str(report_id))
    _scores = []
    for k in range(len(df_qa.paragraph)-1):
        _scores.append(get_scores(df_md, df_qa, df_qa.paragraph[k]))
    return _scores


__all__ = ["get_scores", "digest_report"]

