import {Calendar} from './charts/calendar.js';
import {LineChart} from './charts/lineChart.js';
import {StackedBarChart} from "./charts/stackedBarChart.js";
import {BarChart} from "./charts/barChart.js";
import notebook from "./charts/charts.js";

import {Inspector, Runtime} from "https://cdn.jsdelivr.net/npm/@observablehq/runtime@5/dist/runtime.js";

function updateProgress(analysisId) {
    // Make a GET request to the progress endpoint
    $.get(`/status/${analysisId}`, function (response) {
        const progress = response.progress;
        const message = response.message;

        // If the progress is not 100%, continue updating the progress
        if (progress < 100) {
            $('.progress-bar').css('width', progress + '%');
            $('#count').text(progress + '%');
            $('#message').text(message);

            setTimeout(updateProgress, 100, analysisId);
        } else {
            $('.progress-bar').className += ' done';
            $("#progress").hide();
            $("#results").show();
            displayResults(analysisId)
        }
    });
}

function displayResults(analysisId) {
    $.get(`/analysis/${analysisId}/raw`, response => {
        $("#results header p").text(analysisId);

        displayOverview(response);
        displayMessagesRace(response);

        const topAuthors = Object.entries(response.authors)
            .map(([author, data]) => ({author, total: data.stats.total}))
            .sort((a, b) => b.total - a.total)
            .slice(0, 10);

        const newAuthors = {};
        topAuthors.forEach(entry => newAuthors[entry.author] = response.authors[entry.author]);
        response.authors = newAuthors;

        displayUsers(response);
        displayMessagesGraph(response);
        displayCalendar(response)
        displayAvgWords(response);
        displayAvgMessages(response);
    });
}

function displayOverview(response) {
    $("#messages > h4").text(response.stats.total.toLocaleString());
    $("#media > h4").text(response.stats.media.toLocaleString());
    $("#links > h4").text(response.stats.links.toLocaleString());
}

function displayUsers(response) {
    const types = ["Text", "Media", "Links"];
    const numMessagesTyped = Object.entries(response.authors).map(([author, data]) => {
        return [
            {"author": author, "type": "Text", "amount": data.stats.total - data.stats.media - data.stats.links},
            {"author": author, "type": "Media", "amount": data.stats.media},
            {"author": author, "type": "Links", "amount": data.stats.links}
        ];
    }).flat();

    $("#user-messages").html(StackedBarChart(numMessagesTyped, {
        x: d => d.amount,
        y: d => d.author,
        z: d => d.type,
        marginLeft: 80,
        xLabel: "Messages →",
        yDomain: d3.groupSort(numMessagesTyped, D => d3.sum(D, d => d.amount), d => d.author), // sort y by x
        zDomain: types,
        colors: d3.schemeSpectral[types.length],
    }));

    const maxAuthor = Object.entries(response.authors)
        .map(([author, data]) => ({author, total: data.stats.total}))
        .reduce((a, b) => a.total > b.total ? a : b);
    $("#yapper > h4").text(maxAuthor.author);
    $("#yapper > p > strong").text(maxAuthor.total.toLocaleString());
}

function displayMessagesGraph(response) {
    const data = Object.entries(response.msgsPerDay).map(([key, value]) => {
        const [month, day, year] = key.split('/');
        const date = new Date(year, month - 1, day);
        return {"date": date, "amount": value};
    }).sort((a, b) => a.date - b.date);

    $("#messages-graph").html(LineChart(data, {
        x: d => d.date,
        y: d => d.amount
    }));
}

function displayCalendar(response) {
    const data = Object.entries(response.msgsPerDay).map(([key, value]) => {
        const [month, day, year] = key.split('/');
        const date = new Date(year, month - 1, day);
        return {"date": date, "amount": value};
    }).sort((a, b) => a.date - b.date);

    $("#calendar").html(Calendar(data, {
        x: d => d.date,
        y: d => d.amount,
        cellSize: 15
    }));
}

function displayAvgWords(response) {
    const avgWords = Object.entries(response.authors).map(([author, data]) => ({
        "author": author,
        "amount": data.stats.avgWords
    }));

    $("#avgWords").html(BarChart(avgWords, {
        x: d => d.amount,
        y: d => d.author,
        marginLeft: 80,
        yDomain: d3.groupSort(avgWords, ([d]) => -d.amount, d => d.author), // sort by descending amount
        xLabel: "Average Words Per Message →",
        color: "steelblue"
    }));
}

function displayAvgMessages(response) {
    const avgMsgs = Object.entries(response.authors).map(([author, data]) => ({"author": author, "amount": data.stats.avgMsgsPerDay}));

    $("#avgMsgs").html(BarChart(avgMsgs, {
        x: d => d.amount,
        y: d => d.author,
        marginLeft: 80,
        yDomain: d3.groupSort(avgMsgs, ([d]) => -d.amount, d => d.author), // sort by descending amount
        xLabel: "Average Messages Per Day →",
        color: "steelblue"
    }));
}

function displayMessagesRace(response) {
    const runtime = new Runtime();
    const main = runtime.module(notebook, name => {
        if (["chart", "viewof replay"].includes(name))
            return new Inspector(document.querySelector(`[name='${name}']`));
    });

    const endDate = Object.entries(response.authors)
        .map(([_, data]) => new Date(Object.keys(data.msgsPerDay).splice(-1)[0]))
        .reduce((a, b) => a > b ? a : b);
    const raceData = Object.entries(response.authors).flatMap(([author, data]) => {
        const res = [];
        let totalMessages = 0;
        for (let d = new Date(Object.keys(data.msgsPerDay)[0]); d <= endDate; d.setDate(d.getDate() + 1)) {
            const formatted = `${(d.getMonth() + 1).toString().padStart(2, '0')}/${d.getDate().toString().padStart(2, '0')}/${d.getFullYear()}`;
            const value = data.msgsPerDay[formatted];
            totalMessages += value ? value : 0;
            res.push({"date": new Date(d), "name": author, "category": author, "value": totalMessages})
        }
        return res;
    }).sort((a, b) => a.date - b.date);

    main.redefine("data", raceData);
}

// Call updateProgress when the page is loaded
$(document).ready(function () {
    const analysisID = window.location.pathname.split('/').splice(-1)[0]; // Get the task ID from the URL
    updateProgress(analysisID);
});