from multiprocessing import Pool
import os
import xml.etree.ElementTree as ET
import pandas as pd
import neo4j
from tqdm import tqdm
import logging
import logging.config

pubmed_url = "ftp://ftp.ncbi.nlm.nih.gov/pubmed/baseline/"
directory = "data/"

neo4j_uri = "bolt://localhost:7687"
neo4j_auth = ("neo4j","pubmed")
batch_size = 10000

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('root')

class PubmedException(Exception):
   def __init__(self, desc, file_number):
      self.desc = desc
      self.file_number = file_number

def download_to_xml(file_number):
   filename = "pubmed21n" + str(file_number).zfill(4) + ".xml.gz"

   os.system("wget -q " + pubmed_url + filename + " -P " + directory)
   os.system("wget -q " + pubmed_url + filename + ".md5 -P " + directory)

   stream = os.popen("md5sum " + directory + filename)
   os_output = stream.read()
   downloaded_md5 = os_output.split("  ")[0]
   stream = os.popen("cat " + directory + filename + ".md5")
   os_output = stream.read()
   verified_md5 = os_output.rstrip().split("= ")[1]

   if downloaded_md5 == verified_md5:
      logger.info("File number " + str(file_number) + ", xml checksum matches")
   else:
      logger.error("File number " + str(file_number) + ", xml checksum does not match")
      raise Exception("Checksum discrepancy")

   os.system("rm " + directory + filename + ".md5")
   os.system("gunzip -f " + directory + filename)

def xml_to_df(file_number):
   filename = "pubmed21n" + str(file_number).zfill(4) + ".xml"
   tree = ET.parse(directory + filename)
   root = tree.getroot()

   articles_dict = {}
   for i in tqdm(range(len(root))):
      article_dict = {}

      for article_title in root[i].iter('ArticleTitle'):
         article_dict['Title'] = article_title.text if article_title is not None else ""
      for pubmed_date in root[i].iter('PubMedPubDate'):
         month_text = pubmed_date.find('Month').text.zfill(2) if pubmed_date.find('Month') is not None else ""      
         day_text = pubmed_date.find('Day').text.zfill(2) if pubmed_date.find('Day') is not None else ""
         year_text = pubmed_date.find('Year').text if pubmed_date.find('Year') is not None else ""
         if pubmed_date.attrib['PubStatus'] == 'pubmed':
            article_dict['Date'] = month_text + "/" + day_text + "/" + year_text
      for abstract in root[i].iter('Abstract'):
         article_dict['Abstract'] = abstract.find('AbstractText').text if abstract.find('AbstractText') is not None else ""
      for journal in root[i].iter('ISOAbbreviation'):
         article_dict['Journal'] = journal.text if journal is not None else ""
      
      author_string = ""
      for author_list in root[i].iter('AuthorList'):
         for author in author_list.findall('Author'):
            lastname = author.find('LastName')
            initials = author.find('Initials')
            name = ""
            if lastname is not None:
               name = lastname.text if lastname.text is not None else ""
               if initials is not None:
                  if initials.text is not None:
                     name = name + " " + initials.text
            author_string = author_string + name + ", "
         author_string = author_string[:-2]
      article_dict['Authors'] = author_string

      for language in root[i].iter('Language'):      
         article_dict['Language'] = language.text if language.text is not None else ""

      country = root[i].find('MedlineCitation').find('MedlineJournalInfo').find('Country')
      article_dict['Country'] = country.text if country is not None else ""

      articleid_list = root[i].find('PubmedData').find('ArticleIdList')
      if articleid_list is not None:
         for artid in articleid_list.findall('ArticleId'):
            if artid.attrib['IdType'] == 'pubmed':
               article_id = artid.text if artid is not None else ""
               #article_dict['ArticleID'] = articleid.text
      article_dict['Citations'] = []
      for ref_list in root[i].iter('ReferenceList'):
         for articleid in ref_list.iter('ArticleId'):
            articleid_text = articleid.text if articleid is not None else ""
            if articleid_text is not None and articleid_text != "":
               article_dict['Citations'].append(articleid_text)
 
      major_topics = ""
      minor_topics = ""
      for desc_name in root[i].iter('DescriptorName'):
         if desc_name.attrib['MajorTopicYN'] == 'Y':
            desc_name_text = desc_name.text if desc_name is not None else ""
            major_topics = major_topics + desc_name_text + ", "
         elif desc_name.attrib['MajorTopicYN'] == 'N':
            desc_name_text = desc_name.text if desc_name is not None else ""
            minor_topics = minor_topics + desc_name_text + ", "
      article_dict['MajorTopics'] = major_topics[:-2]
      article_dict['MinorTopics'] = minor_topics[:-2]

      if article_id is None or article_id == "" or int(article_id) < 0:
         logger.warn("File number " + str(file_number) + ", missing article_id")
         continue 

      if article_id in articles_dict:
         logger.warn("File number " + str(file_number) + ", duplicate article_id: " + str(article_id))
   
      articles_dict[article_id] = article_dict

   os.system("rm " + directory + filename)

   articles_df = pd.DataFrame.from_dict(articles_dict, orient='index')
   return articles_df

def df_to_db(articles_df):
   driver = neo4j.GraphDatabase.driver(neo4j_uri, auth=neo4j_auth)
   session = driver.session()
   result = session.run("CREATE CONSTRAINT IF NOT EXISTS ON (n:Article) ASSERT n.ArticleId IS UNIQUE")
   tx = session.begin_transaction()
   node_query = "MERGE (a: Article {ArticleId: $id }) RETURN a.ArticleId"
   rel_query = ("MERGE (a:Article {ArticleId: $id1}) " +
                "MERGE (b:Article {ArticleId: $id2}) " +
                "MERGE (b)-[:CITES]->(a) " +
                "RETURN b.ArticleId, a.ArticleId")
   for i in tqdm(range(len(articles_df.index))):
      article_id = articles_df.index[i]
      result = tx.run(node_query, {'id': article_id})
      for citing_id in articles_df.loc[article_id, 'Citations']:
         result = tx.run(rel_query, {'id1': article_id, 'id2': citing_id})
      if i % batch_size == batch_size - 1:
         tx.commit()
         tx.close()
         tx = session.begin_transaction()
   tx.commit()
   tx.close()
   session.close()
   driver.close()


def download_to_db(file_number):
   try:
      logger.info("File number " + str(file_number) + ", beginning xml download")
      download_to_xml(file_number)
      logger.info("File number " + str(file_number) + ", completed xml download")
   except Exception as e:
      logger.error("File number " + str(file_number) + ", " + str(e))
      raise PubmedException("XML download failed", file_number)
   try:
      logger.info("File number " + str(file_number) + ", beginning parsing xml")   
      articles_df = xml_to_df(file_number)
      logger.info("File number " + str(file_number) + ", completed parsing xml")
   except Exception as e:
      logger.error("File number " + str(file_number) + ", " + str(e))
      raise PubmedException("XML parsing failed", file_number)
   try:
      logger.info("File number " + str(file_number) + ", beginning neo4j ingestion")
      df_to_db(articles_df)
      logger.info("File number " + str(file_number) + ", completed neo4j ingestion")
   except Exception as e:
      logger.error("File number " + str(file_number) + ", " + str(e))
      raise PubmedException("Neo4j ingestion failed", file_number)

if __name__ == "__main__":
   for file_number in range(1,1063):
      try: 
         #with Pool(1) as p:
            #p.map(download_to_db, list(range(1,1063)))
         download_to_db(file_number)
      except Exception as e:
         logger.error("File number " + str(e.file_number) + ", incomplete, " + e.desc)

