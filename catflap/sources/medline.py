import xml.etree.cElementTree as cetree
import json, os, zipfile
from catflap import Journal

MEDLINE_ZIP_DIR = "/home/richard/MedLine/zip_files/"
# MEDLINE_XML_DIR = "/home/richard/MedLine/xml/"
PMID_DIR = "/home/richard/MedLine/pmid/"

def list_zips():
    return [os.path.join(MEDLINE_ZIP_DIR, f) for f in os.listdir(MEDLINE_ZIP_DIR) if f.endswith(".zip")]

def list_files():
    return [os.path.join(MEDLINE_XML_DIR, f) for f in os.listdir(MEDLINE_XML_DIR) if f.endswith("xml")]

def pmid_filepath(zip_filepath):
    return os.path.join(PMID_DIR, os.path.split(zip_filepath)[1].split(".")[0] + ".pmid.txt")

def extract_data(filepath, pmidfile):
    fn = os.path.split(filepath)[1]
    
    # extract the xml from the zip
    z = zipfile.ZipFile(filepath)
    inf = z.filelist[0]
    f = z.open(inf)
    
    # parse the xml
    tree = cetree.parse(f)
    root = tree.getroot()

    pmids = []
    records = []
    for mlc in root.findall("MedlineCitation"):
        record = {"electronic_issn" : [], "print_issn" : [], "issn" : [], "journal_name" : [], "journal_abbr" : []}
        has_data = False
        
        # record any pubmed ids that we encounter
        mypmid = None
        pmidels = mlc.findall("PMID")
        for e in pmidels:
            mypmid = e.text
            if e.text not in pmids:
                pmids.append(e.text)
        
        article = mlc.find("Article")
        journal = article.find("Journal")
        
        # record any ISSNs in their appropriate type field
        issnels = journal.findall("ISSN")
        for e in issnels:
            has_data = True
            if e.get("IssnType") == "Electronic":
                if e.text not in record["electronic_issn"]:
                    record["electronic_issn"].append(e.text)
                if e.text in record["issn"]:
                    record["issn"].remove(e.text)
            elif e.get("IssnType") == "Print":
                if e.text not in record["print_issn"]:
                    record["print_issn"].append(e.text)
                if e.text in record["issn"]:
                    record["issn"].remove(e.text)
            else:
                if e.text not in record["issn"]:
                    record["issn"].append(e.text)
        
        # add journal titles
        titlels = journal.findall("Title")
        for e in titlels:
            has_data = True
            if e.text not in record["journal_name"]:
                record["journal_name"].append(e.text)
        
        # add journal abbreviations
        isoels = journal.findall("ISOAbbreviation")
        for e in isoels:
            has_data = True
            if e.text not in record["journal_abbr"]:
                record["journal_abbr"].append(e.text)
        
        info = mlc.find("MedlineJournalInfo")
        
        # record medline's version of the name (which may well be the iso abbreviation)
        taels = info.findall("MedlineTA")
        for e in taels:
            has_data = True
            if e.text not in record["journal_name"] and e.text not in record["journal_abbr"]:
                record["journal_abbr"].append(e.text)
        
        # record the medline knowledge of this other issn - we don't necessarily know what type it is
        links = info.findall("ISSNLinking")
        for e in links:
            has_data = True
            if e.text not in record["electronic_issn"] and e.text not in record["print_issn"] and e.text not in record["issn"]:
                record["issn"].append(e.text)
        
        # now write it to the ACAT via catflap
        if has_data:
            jnames = record.get("journal_name", [])
            for a in record.get("journal_abbr", []):
                if a not in jnames:
                    jnames.append(a)
            source = "medline " + fn
            if mypmid is not None:
                source += " " + mypmid
            Journal.add(issn=record.get("issn", []) + record.get("electronic_issn", []) + record.get("print_issn", []),
                        journal_name=jnames,
                        electronic_issn=record.get("electronic_issn", []),
                        print_issn=record.get("print_issn", []),
                        journal_abbreviation=record.get("journal_abbr", []),
                        source=source)
    
    # save the pmids found in this file
    save_pmids(pmidfile, pmids)

def save_pmids(pmidfile, pmids):
    f = open(pmidfile, "w")
    f.write("\n".join(pmids))
    f.close()

if __name__ == "__main__":
    files = list_zips()
    for f in files:
        print "processing file ", f
        pmidpath = pmid_filepath(f)
        extract_data(f, pmidpath)
