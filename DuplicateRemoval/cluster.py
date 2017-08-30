#!/usr/bin/env python2.7
# -*- coding: utf8 -*-

import csv
import json
import pprint
from acronym import *
from levenshtein import *

org_types = ['engineering', 'foundation', 'law office', 'association']

class DDOrgTools:
    # stoplist = [',', 'of', 'california', 'inc', 'association', '&', 'pac', 'and', 'for',
    #             'the', 'llc', 'school', 'group', 'city', 'ca', 'committee', 'community',
    #             'corporation', 'union', 'associates', 'llp', 'foundation', 'inc.',
    #             'assn', 'a', 'assoc', 'co', 'corp', '#', 'at', 'cal', 'by', 'on', ':', ';', "enterprise",
    #             "enterprises", "systems"]

    stoplist = ['llc', 'pac', 'inc', 'foundation', 'engineering', 'law office', 'corporation', 'corp']

    def __init__(self):
        pass

    def preprocess(self, content):
        data = list()
        for line in content:
            if re.match('[a-zA-Z0-9_]+', line):
                data.append(line)
        return data

    def filterdata(self, line):
        line = line.lower()

        for char in line:
            if not (char.isalnum() or char.isspace()):
                line = line.replace(char, '')

        linewords = line.split(' ')
        if linewords[0] == 'u':
            linewords.pop(0)

        for word in linewords:
            if word in self.stoplist:
                linewords.remove(word)

        line = ' '.join(linewords)

        for m in re.finditer(r'(llc|inc|pac|assn|political action committee)( |$)', line):
            line = line.replace(m.group(0).strip(), ' ')

        return line.strip()

    def bracesRemove(self, line):
        for m in re.finditer(r'(\(|\[).*?(\]|\))', line):
            line = line.replace(m.group(), '')
        return line

    def getDistance(self, org1, org2):
        org1 = self.filterdata(org1)
        org1 = self.bracesRemove(org1)
        org2 = self.filterdata(org2)
        org2 = self.bracesRemove(org2)
        #print([org1, org2])
        list1 = re.split('\s+', org1)
        list2 = re.split("\s+", org2)
        word_distance = levenshtein(re.split('\s+', org1), re.split("\s+", org2))
        char_distance = levenshtein(org1.replace(" ", ""), org2.replace(" ", ""))
        if word_distance != 0:
            word_distance = float(word_distance) / max(len(list1), len(list2))
        return word_distance
        # import math
        # return math.pow(100, word_distance)

    def _findAcronyms(self, org, targetOrg, matcher):
        org1Matches = list(matcher.finditer(targetOrg))

        if org1Matches:
            org2FirstChars = ''.join([w[0] for w in org.lower().split(' ')
                                      if len(w) > 0])
            for match in org1Matches:
                acronymChars = re.sub(r'\.', '', match.group().lower())
                if len(acronymChars) > 2 and acronymChars in org2FirstChars:
                    print('Matched acronmym {} in {} to {}'.format(match.group(), targetOrg, org))

    def tryExpandAcronyms(self, org1, org2):
        acronymRE = re.compile(r'((?:[A-Z]\.)+)')

        org1Matches = self._findAcronyms(org1, org2, acronymRE)
        org2Matches = self._findAcronyms(org2, org1, acronymRE)

        if org1Matches:
            pass
        if org2Matches:
            pass

    def getDistanceCombined(self, org1, org2, locationDf):
        # TODO: if one org is a subset of other org, should be very close match
        oid_1, org1 = org1
        oid_2, org2 = org2

        org1 = str(org1)
        org2 = str(org2)

        org_one_locs = locationDf[locationDf.oid == oid_1]
        org_two_locs = locationDf[locationDf.oid == oid_2]

        if not org_one_locs.empty:
            print('Org one locs:', org_one_locs)
        if not org_two_locs.empty:
            print('Org two locs:', org_two_locs)
        # self.tryExpandAcronyms(org1, org2)
        # print(org1)
        # print(org2)
        org1 = self.filterdata(org1)
        org1 = self.bracesRemove(org1)
        org2 = self.filterdata(org2)
        org2 = self.bracesRemove(org2)
        list1 = re.split('\s+', org1)
        list2 = re.split("\s+", org2)
        word_distance = levenshtein(list1, list2)
        if word_distance != 0:
            word_distance = float(word_distance) / max(len(list1), len(list2))
        org1 = org1.replace(" ", "")
        org2 = org2.replace(" ", "")
        char_distance = levenshtein(org1, org2)
        if char_distance != 0:
            char_distance = float(char_distance) / max(len(org1), len(org2))
        distance = word_distance * char_distance
        return distance


def stage3_cluster(clusters):
    s3_clusters = []
    s2_clusters = [{'clustered': 0, 'name': key} for key in clusters.keys()]

    for i in range(0, len(s2_clusters)):
        if s2_clusters[i]['clustered'] == 0:
            so_clust = [s2_clusters[i]]

            for j in range(i+1, len(s2_clusters)):
                word_dist = levenshtein(s2_clusters[i]['name'], s2_clusters[j]['name'])

                if s2_clusters[j]['clustered'] == 0 and word_dist <= 1:
                    so_clust.append(s2_clusters[j])
                    s2_clusters[j]['clustered'] = 1

            s3_clusters.append(so_clust)
            s2_clusters[i]['clustered'] = 1

    print(len(s3_clusters))
    s3_cluster_dict = {}
    for cluster in s3_clusters:
        s3_cluster_dict[cluster[0]['name']] = []

        for subcluster in cluster:
                s3_cluster_dict[cluster[0]['name']] += clusters[subcluster['name']]

    return s3_cluster_dict


def stage2_cluster(clusters):
    s2_clusters = []
    s1_clusters = [{'clustered': 0, 'name': key} for key in clusters.keys()]

    for i in range(0, len(s1_clusters)):
        if s1_clusters[i]['clustered'] == 0:
            so_clust = [s1_clusters[i]]
            cluster_words = s1_clusters[i]['name'].split(' ')

            for j in range(i+1, len(s1_clusters)):
                cluster2_words = s1_clusters[j]['name'].split(' ')
                word_dist = levenshtein(cluster_words, cluster2_words)

                if s1_clusters[j]['clustered'] == 0 and word_dist <= 1\
                        and (len(cluster_words) > 1 and len(cluster2_words) > 1):
                    so_clust.append(s1_clusters[j])
                    s1_clusters[j]['clustered'] = 1

            s2_clusters.append(so_clust)
            s1_clusters[i]['clustered'] = 1

    print(len(s2_clusters))
    s2_cluster_dict = {}
    for cluster in s2_clusters:
        s2_cluster_dict[cluster[0]['name']] = []

        for subcluster in cluster:
                s2_cluster_dict[cluster[0]['name']] += clusters[subcluster['name']]

    return s2_cluster_dict


def stage1_cluster(orgs):
    cluster_tool = DDOrgTools()

    clusters = {}

    for i in range(0, len(orgs)):
        if orgs[i]['clustered'] == 0:
            canon_name = cluster_tool.filterdata(orgs[i]['name'])
            clusters[canon_name] = [orgs[i]]
            orgs[i]['clustered'] = 1

        for j in range(i+1, len(orgs)):
            dist = cluster_tool.getDistance(orgs[i]['name'], orgs[j]['name'])
            if orgs[j]['clustered'] == 0 and dist == 0:
                clusters[canon_name].append(orgs[j])
                orgs[j]['clustered'] = 1

    return clusters


def main():
    with open('OrgClusterTest.csv', 'r') as csvfile:
        csv_reader = csv.reader(csvfile)
        csv_reader.next()

        orgs = [{'oid': row[1], 'name': row[2], 'clustered': 0} for row in csv_reader]

    s1_clusters = stage1_cluster(orgs)
    # s2_clusters = stage2_cluster(s1_clusters)
    # s3_clusters = stage3_cluster(s2_clusters)
    # pprint.pprint(s3_clusters)

    with open('test_clusters.json', 'wb') as jsonfile:
        json.dump(s1_clusters, jsonfile)

    # with open('s2_test_clusters.json', 'wb') as jsonfile:
    #     json.dump(s2_clusters, jsonfile)
    #
    # with open('s3_test_clusters.json', 'wb') as jsonfile:
    #     json.dump(s3_clusters, jsonfile)


if __name__ == '__main__':
    main()
