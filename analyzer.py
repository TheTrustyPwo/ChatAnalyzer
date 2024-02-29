import os
import re
import shutil
from collections import Counter
from datetime import datetime
from threading import Thread

import numpy as np
import pandas as pd

from app import mongo

DEFAULT_DATEFORMAT = "%d/%m/%Y"
pd.options.mode.chained_assignment = None


class Analyzer(Thread):
    def __init__(self, analysis_id: str):
        super().__init__()
        self.analysis_id = analysis_id
        self.path = os.path.join('data', analysis_id)
        self.data = {"_id": self.analysis_id, "authors": {}}
        self.messages = []
        self.df = None
        self.messages_df = None
        self.media_messages_df = None
        self.authors = []

        self.progress = 0
        self.update_progress("Initializing")
        os.makedirs(self.path)

    def run(self) -> None:
        self.load_all()
        self.compile_data()
        self.save()
        shutil.rmtree(self.path)

    def update_progress(self, message: str):
        document = {"_id": self.analysis_id, "progress": int(self.progress), "message": message}
        mongo.db.progress.update_one({"_id": self.analysis_id}, {"$set": document}, upsert=True)

    @staticmethod
    def get_datapoint(line: str):
        match = re.search('^(\d{1,4}/\d{1,2}/\d{1,4}), (\d+:\d+) - (.[^"]*?): (.*)', line)
        if match is None:
            return line.strip(),
        date, time, author, message = match.groups()
        return date, time, author, message

    def get_datetime_format(self, filename: str):
        formats = ["%d/%m/%y", "%m/%d/%y", "%d/%m/%Y"]
        with open(filename, encoding='utf-8') as fp:
            lines = fp.readlines()
            for line in lines:
                date = re.search('^(\d{1,4}/\d{1,2}/\d{1,4})', line)
                if date is None:
                    continue
                for i, format in enumerate(formats):
                    try:
                        datetime.strptime(date.group(1), format)
                    except ValueError:
                        # Deletion in O(1)
                        formats[i], formats[-1] = formats[-1], formats[i]
                        formats.pop()
                if len(formats) == 1:
                    break
        return formats[0]

    def load(self, filename: str):
        datetime_format = self.get_datetime_format(filename)
        date, time, author = None, None, None
        message_buffer = []

        with open(filename, encoding='utf-8') as fp:
            lines = fp.readlines()

            date_start = datetime.strptime(lines[0].split(',')[0], datetime_format)
            date_end = datetime.strptime(lines[-1].split(',')[0], datetime_format)

            for line in lines[1:]:
                message_data = self.get_datapoint(line)

                if len(message_data) == 1:
                    if any(i in line for i in (
                            'left', 'created group', 'Messages and calls are end-to-end encrypted', 'changed the',
                            'added')):
                        continue
                    message_buffer.append(message_data[0])
                else:
                    if len(message_buffer) > 0 and date is not None:
                        date_parsed = datetime.strptime(date, datetime_format)
                        self.messages.append([datetime.strftime(date_parsed, DEFAULT_DATEFORMAT), time, author,
                                              ' '.join(message_buffer)])
                    message_buffer.clear()
                    date, time, author, message = self.get_datapoint(line)
                    try:
                        date_parsed = datetime.strptime(date, datetime_format)
                        if date_start <= date_parsed <= date_end:
                            message_buffer.append(message)
                    except ValueError:
                        pass

    def load_all(self):
        files = os.listdir(self.path)
        for file in files:
            if file[-3:] != 'txt':
                continue
            self.load(os.path.join(self.path, file))

        self.df = pd.DataFrame(self.messages, columns=["Date", 'Time', 'Author', 'Message'])
        self.authors = self.df.Author.unique()
        self.df['Date'] = pd.to_datetime(self.df['Date'], format=DEFAULT_DATEFORMAT)
        self.df['urlcount'] = self.df.Message.apply(lambda x: re.findall('(https?://\S+)', x)).str.len()

        self.media_messages_df = self.df[self.df['Message'] == '<Media omitted>']
        self.messages_df = self.df.drop(self.media_messages_df.index)
        self.messages_df['Letter_Count'] = self.messages_df['Message'].apply(lambda s: len(s))
        self.messages_df['Word_Count'] = self.messages_df['Message'].apply(lambda s: len(s.split(' ')))
        self.messages_df["MessageCount"] = 1

    def stats(self, author=None):
        req_df = self.df if author is None else self.df[self.df.Author == author]
        resampled = req_df.set_index('Date').resample('D').count().fillna(0)
        req_messages_df = self.messages_df.copy() if author is None else self.messages_df[
            self.messages_df.Author == author]
        req_media_messages_df = self.media_messages_df.copy() if author is None else self.media_messages_df[
            self.media_messages_df.Author == author]
        return {
            "total": req_df.shape[0],
            "media": req_media_messages_df.shape[0],
            "links": sum(req_messages_df["urlcount"]),
            "avgWords": np.sum(req_messages_df['Word_Count']) / max(req_messages_df.shape[0], 1),
            "avgMsgsPerDay": resampled.Message.mean(),
            "mostSent": {
                "date": resampled.Message.idxmax().strftime('%m/%d/%Y'),
                "amount": int(resampled.loc[resampled.Message.idxmax(), 'Message'])
            }
        }

    def frequency(self, author=None, limit=50):
        req_df = self.messages_df.copy() if author is None else self.messages_df[self.messages_df.Author == author]
        frequency_map = Counter(' '.join(req_df.Message).split()).most_common(limit)
        return {word: freq for word, freq in frequency_map}

    def messages_per_day(self, author=None):
        req_df = self.df if author is None else self.df[self.df.Author == author]
        grouped = req_df.groupby('Date').count().Message.to_dict()
        return {key.strftime('%m/%d/%Y'): value for key, value in grouped.items()}

    def messages_per_day_of_week(self, author=None):
        req_df = self.df if author is None else self.df[self.df.Author == author]
        req_df['Day'] = req_df['Date'].dt.day_name()
        pivot = req_df.pivot_table(index='Day', values='Message', aggfunc='count')
        pivot = pivot.reindex(['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
        pivot = pivot.fillna(0)
        return pivot.to_dict(orient='dict')['Message']

    def messages_per_hour_of_day(self, author=None):
        req_df = self.df if author is None else self.df[self.df.Author == author]
        req_df['Hour'] = pd.to_datetime(req_df['Time'], format='%H:%M').dt.hour
        pivot = req_df.pivot_table(index='Hour', values='Message', aggfunc='count')
        return {str(key): value for key, value in pivot.to_dict(orient='dict')['Message'].items()}

    def compile_data(self):
        authors = self.df.Author.unique()

        self.update_progress("Analyzing Global Statistics")
        self.data["stats"] = self.stats()
        self.data["frequency"] = self.frequency()
        self.data["msgsPerDay"] = self.messages_per_day()
        self.data["msgsPerDayOfWeek"] = self.messages_per_day_of_week()
        self.data["msgsPerHourOfDay"] = self.messages_per_hour_of_day()
        self.progress += 99 / (1 + len(self.authors))

        for author in authors:
            self.update_progress(f"Analyzing {author}")
            self.data["authors"][author] = {
                "stats": self.stats(author),
                "frequency": self.frequency(author),
                "msgsPerDay": self.messages_per_day(author),
                "msgsPerDayOfWeek": self.messages_per_day_of_week(author),
                "msgsPerHourOfDay": self.messages_per_hour_of_day(author)
            }
            self.progress += 99 / (1 + len(self.authors))

        self.update_progress("Analysis Complete")

    def save(self):
        self.update_progress("Saving")
        mongo.db.analyses.insert_one(self.data)
        self.progress = 100
        self.update_progress("Complete")
