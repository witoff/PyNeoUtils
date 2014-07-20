from py2neo import neo4j, node, rel
import sys


def getLargestId(db, label):
  print 'Note: This only works with integer fields, NOT strings'
  q = neo4j.CypherQuery(db, "match (n:%s) return n.id order by n.id desc limit 1" % label)
  r = q.execute()
  return r.data[0].values[0]


def mergeNodes(db, label, data, id_key='id', uniqueIndex=True):
  """
  Create Batch Nodes in the database
    db: A py2neo DB Object
    label:  The label to apply to these nodes
            e.g. "Person"
    data:   An array of dicts to add to these objects
            e.g. [
                   {attr: 1},
                   {attr: 2},
                   ...
                 ]
    key:    The key in each dict item to index on.  Key will be renamed to 'id'
            e.g. 'person_id'
  """

  " MERGE (n:Test { id:2 }) "
  "   ON CREATE SET n.name = 'new', n.new = 'yes' "
  "   ON MATCH SET n.name = 'rob', n.something = 'more' "
  "   SET n.always = 'yup!' "
  " RETURN n "
  print 'Merging nodes for label: ', label

  # Create Index
  if uniqueIndex:
    #N.B. Creating a constraint also create an index
    query = neo4j.CypherQuery(db, "CREATE CONSTRAINT ON (n:%s) ASSERT n.id IS UNIQUE" % label)
    query.execute()
    # Remove with DROP CONSTRAINT ON (n:%s) ASSERT n.id IS UNIQUE
    pass
  else:
    query = neo4j.CypherQuery(db, "CREATE INDEX ON :%s(id)" % label)
    query.execute()
    # Can remove with Drop index on :%s(id)

  def getQuery(label, keys):
    query_str = "MERGE (n:%s { id:{id} } ) " % label

    params = []
    for k in keys:
      if k == 'id': continue
      params.append( "n.%s={%s}" % (k, k) )

    if len(params) > 0:
      query_str += " SET "
      query_str += ','.join(params)

    query_str += " RETURN n"

    merge_query = neo4j.CypherQuery(db, query_str)
    return merge_query

  print '..building query'
  batch = neo4j.WriteBatch(db)
  for d in data:
    attributes = d.copy()
    #Rename the ID for the index
    attributes['id'] = int(attributes.pop(id_key))

    #Swap '-' to '_' to make parameter keys legal
    for k in attributes:
      new_k = k.replace('-','_')
      attributes[new_k] = attributes.pop(k)

    query = getQuery(label, attributes.keys())
    batch.append_cypher(query, params=attributes)

  print '..submitting query'
  nodes = batch.submit()
  print '..done'

def createNodes(db, label, data, id_key='id', uniqueIndex=True):
    print 'creating nodes: ', label

    # Create Index
    if uniqueIndex:
      #N.B. Creating a constraint also create an index
      query = neo4j.CypherQuery(db, "CREATE CONSTRAINT ON (n:%s) ASSERT n.id IS UNIQUE" % label)
      query.execute()
    else:
      query = neo4j.CypherQuery(db, "CREATE INDEX ON :%s(id)" % label)
      query.execute()
      # Can remove with Drop index on :%s(id)

    # Write the batch
    batch = neo4j.WriteBatch(db)
    for d in data:
        attributes = d.copy()

        #Rename the ID for hte index
        attributes['id'] = int(attributes.pop(id_key))

        n = batch.create(node(attributes))
        batch.add_labels(n, label)# , 'label2', 'label3'...)
    nodes = batch.submit()



def __getIds(data,key):
  """Given a dictionary key, return an array string ids

  @data -- dict containing some ids
  @key  -- key in the dict that is convertible to string or a list
  @return -- an array of string ids

  """

  values  = data.get(key)
  if key not in data:
    print 'warning, key does not exist', key
    return []
  if values == None:
    return []
  if not isinstance(values, list):
    return [ str(values) ]
  return [str(v) for v in values]


def createRelationships(db, data, source_key, target_key, source_label, target_label, rel_label ):
    print 'creating relationshinps from %s to %s' % (source_label, target_label)

    query = "MATCH (s:%s),(t:%s) "\
            " WHERE s.id = %s and t.id = %s "\
            " CREATE (s)-[:%s]->(t)" % (source_label, target_label, "%s", "%s", rel_label)
    print 'executing query: ', query
    sys.stdout.flush()

    batch = neo4j.WriteBatch(db)
    for d in data:
        targets = __getIds(d, target_key)
        sources = __getIds(d, source_key)

        for t in targets:
          for s in sources:
            batch.append_cypher(query % (s, t))

    batch.submit()


def deleteLabels(db, label):
  query = neo4j.CypherQuery(db, "MATCH (n:%s) DELETE n" % label)
  query.execute()

def deleteAll(db):
    query = neo4j.CypherQuery(db, "MATCH (n) OPTIONAL MATCH (n)-[r]-() DELETE n,r")
    query.execute()
