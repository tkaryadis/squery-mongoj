/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.bson;

import clojure.java.api.Clojure;
import clojure.lang.*;
import clojure.lang.Compiler;
import org.bson.codecs.Decoder;
import org.bson.codecs.DecoderContext;
import org.bson.codecs.DocumentCodec;
import org.bson.codecs.Encoder;
import org.bson.codecs.EncoderContext;
import org.bson.codecs.configuration.CodecRegistry;
import org.bson.conversions.Bson;
import org.bson.json.JsonMode;
import org.bson.json.JsonReader;
import org.bson.json.JsonWriter;
import org.bson.json.JsonWriterSettings;
import org.bson.types.ObjectId;

import java.io.Serializable;
import java.io.StringWriter;
import java.util.*;
import java.util.function.Consumer;

import static java.lang.String.format;
import static org.bson.assertions.Assertions.isTrue;
import static org.bson.assertions.Assertions.notNull;

//i will make document implement all interfaces i need,to be used from the normal clojure functions
//that i want  get/assoc/dissoc etc  (i need Associative interface)
//i will get them from the order-map online code
//i will not make Document an ordered-map,i will just add the functions it needs,using as member the ordered-map
//like Document did to LinkedHashMap
public class Document implements  Map, Serializable, Bson , MapEquivalence  , IPersistentMap ,Associative
{
    public boolean isJDocument()
    {
        return (this.documentAsMapj!=null);
    }

    private static final long serialVersionUID = 6297731997167536582L;

    public LinkedHashMap<String, Object> documentAsMapj;

    //Clojure///////////////////////////////////////////////////
    //static
    //{
    //    IFn require = Clojure.var("clojure.core", "require");
    //    require.invoke(Clojure.read("flatland.ordered.map"));

    //}
    public IPersistentMap documentAsMapclj;
    //static IFn createDocumentAsMapC = Clojure.var("flatland.ordered.map", "ordered-map");
    static IFn dissocC = Clojure.var("clojure.core", "dissoc");
    static IFn mergeC = Clojure.var("clojure.core", "merge");
    static IFn seqC = Clojure.var("clojure.core", "seq");
    ////////////////////////////////////////////////////////////

    //Default is a java document
    public Document()
    {
        documentAsMapj = new LinkedHashMap<String, Object>();
    }

    /**
     * Creates a Document instance initialized with the given map.
     *
     * @param map initial map
     */
    public Document(final Map<String, Object> map)
    {
        documentAsMapj = new LinkedHashMap<String, Object>(map);
    }

    /**
     * Create a Document instance initialized with the given key/value pair.
     *
     * @param key   key
     * @param value value
     */
    public Document(final String key, final Object value)
    {
        documentAsMapj = new LinkedHashMap<String, Object>();
        documentAsMapj.put(key, value);
    }

    //Clojure ones i construct with one extra argument
    //inside it is a HashMap,only exception is the Commands that go to Encode
    //in commands i use Document(true,map) where map is ordered map

    public Document(boolean isClj)
    {
        if(isClj)
        {
            documentAsMapclj = (IPersistentMap) PersistentArrayMap.create(new HashMap()); //createDocumentAsMapC.invoke();
        }
        else documentAsMapj = new LinkedHashMap<String, Object>();
    }

    public Document(boolean isClj,Map map)
    {
        if(isClj) this.documentAsMapclj = (IPersistentMap) map;
        else documentAsMapj = new LinkedHashMap<String, Object>(map);
    }

    public Document(boolean isClj,final String key, final Object value)
    {
        if (isClj)
        {
            Map<Object,Object> m=new HashMap<>();
            m.put(key,value);
            documentAsMapclj = (IPersistentMap) PersistentArrayMap.create(m); //createDocumentAsMapC.invoke(key,value);

        }
        else
        {
            documentAsMapj = new LinkedHashMap<String, Object>();
            documentAsMapj.put(key, value);
        }
    }

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code Document}
     *
     * @param json the JSON string
     * @return a corresponding {@code Document} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static Document parse(final String json)
    {
        return parse(json, new DocumentCodec());
    }

    /**
     * Parses a string in MongoDB Extended JSON format to a {@code Document}
     *
     * @param json the JSON string
     * @param decoder the {@code Decoder} to use to parse the JSON string into a {@code Document}
     * @return a corresponding {@code Document} object
     * @see org.bson.json.JsonReader
     * @mongodb.driver.manual reference/mongodb-extended-json/ MongoDB Extended JSON
     */
    public static Document parse(final String json, final Decoder<Document> decoder)
    {
        notNull("codec", decoder);
        JsonReader bsonReader = new JsonReader(json);
        return decoder.decode(bsonReader, DecoderContext.builder().build());
    }

    @Override
    public <C> BsonDocument toBsonDocument(final Class<C> documentClass, final CodecRegistry codecRegistry)
    {
        return new BsonDocumentWrapper<Document>(this, codecRegistry.get(Document.class));
    }

    /**
     * Gets the value in an embedded document, casting it to the given {@code Class<T>}.  The list of keys represents a path to the
     * embedded value, drilling down into an embedded document for each key. This is useful to avoid having casts in
     * client code, though the effect is the same.
     *
     * The generic type of the keys list is {@code ?} to be consistent with the corresponding {@code get} methods, but in practice
     * the actual type of the argument should be {@code List<String>}. So to get the embedded value of a key list that is of type String,
     * you would write {@code String name = doc.getEmbedded(List.of("employee", "manager", "name"), String.class)} instead of
     * {@code String name = (String) doc.get("employee", Document.class).get("manager", Document.class).get("name") }.
     *
     * @param keys  the list of keys
     * @param clazz the non-null class to cast the value to
     * @param <T>   the type of the class
     * @return the value of the given embedded key, or null if the instance does not contain this embedded key.
     * @throws ClassCastException if the value of the given embedded key is not of type T
     * @since 3.10
     */
    public <T> T getEmbedded(final List<?> keys, final Class<T> clazz)
    {
        notNull("keys", keys);
        isTrue("keys", !keys.isEmpty());
        notNull("clazz", clazz);
        return getEmbeddedValue(keys, clazz, null);
    }

    /**
     * Gets the value in an embedded document, casting it to the given {@code Class<T>} or returning the default value if null.
     * The list of keys represents a path to the embedded value, drilling down into an embedded document for each key.
     * This is useful to avoid having casts in client code, though the effect is the same.
     *
     * The generic type of the keys list is {@code ?} to be consistent with the corresponding {@code get} methods, but in practice
     * the actual type of the argument should be {@code List<String>}. So to get the embedded value of a key list that is of type String,
     * you would write {@code String name = doc.getEmbedded(List.of("employee", "manager", "name"), "John Smith")} instead of
     * {@code String name = doc.get("employee", Document.class).get("manager", Document.class).get("name", "John Smith") }.
     *
     * @param keys  the list of keys
     * @param defaultValue what to return if the value is null
     * @param <T>   the type of the class
     * @return the value of the given key, or null if the instance does not contain this key.
     * @throws ClassCastException if the value of the given key is not of type T
     * @since 3.10
     */
    public <T> T getEmbedded(final List<?> keys, final T defaultValue)
    {
        //System.out.println("Calling getEmbeded()");

        notNull("keys", keys);
        isTrue("keys", !keys.isEmpty());
        notNull("defaultValue", defaultValue);
        return getEmbeddedValue(keys, null, defaultValue);
    }


    // Gets the embedded value of the given list of keys, casting it to {@code Class<T>} or returning the default value if null.
    // Throws ClassCastException if any of the intermediate embedded values is not a Document.
    @SuppressWarnings("unchecked")
    private <T> T getEmbeddedValue(final List<?> keys, final Class<T> clazz, final T defaultValue)
    {
        System.out.println("getEmbeddedValue");
        Object value = this;
        Iterator<?> keyIterator = keys.iterator();
        while (keyIterator.hasNext()) {
            Object key = keyIterator.next();
            value = ((Document) value).get(key);
            if (!(value instanceof Document)) {
                if (value == null) {
                    return defaultValue;
                } else if (keyIterator.hasNext()) {
                    throw new ClassCastException(format("At key %s, the value is not a Document (%s)",
                            key, value.getClass().getName()));
                }
            }
        }
        return clazz != null ? clazz.cast(value) : (T) value;
    }

    /**
     * Gets the value of the given key as an Integer.
     *
     * @param key the key
     * @return the value as an integer, which may be null
     * @throws java.lang.ClassCastException if the value is not an integer
     */
    public Integer getInteger(final Object key)
    {
        return (Integer) get(key);
    }

    /**
     * Gets the value of the given key as a primitive int.
     *
     * @param key          the key
     * @param defaultValue what to return if the value is null
     * @return the value as an integer, which may be null
     * @throws java.lang.ClassCastException if the value is not an integer
     */
    public int getInteger(final Object key, final int defaultValue)
    {
        return get(key, defaultValue);
    }

    /**
     * Gets the value of the given key as a Long.
     *
     * @param key the key
     * @return the value as a long, which may be null
     * @throws java.lang.ClassCastException if the value is not an long
     */
    public Long getLong(final Object key)
    {
        return (Long) get(key);
    }

    /**
     * Gets the value of the given key as a Double.
     *
     * @param key the key
     * @return the value as a double, which may be null
     * @throws java.lang.ClassCastException if the value is not an double
     */
    public Double getDouble(final Object key)
    {
        //System.out.println("Calling getDouble()");
        return (Double) get(key);
    }

    /**
     * Gets the value of the given key as a String.
     *
     * @param key the key
     * @return the value as a String, which may be null
     * @throws java.lang.ClassCastException if the value is not a String
     */
    public String getString(final Object key)
    {
        return (String) get(key);
    }

    /**
     * Gets the value of the given key as a Boolean.
     *
     * @param key the key
     * @return the value as a Boolean, which may be null
     * @throws java.lang.ClassCastException if the value is not an boolean
     */
    public Boolean getBoolean(final Object key)
    {
        return (Boolean) get(key);
    }

    /**
     * Gets the value of the given key as a primitive boolean.
     *
     * @param key          the key
     * @param defaultValue what to return if the value is null
     * @return the value as a primitive boolean
     * @throws java.lang.ClassCastException if the value is not a boolean
     */
    public boolean getBoolean(final Object key, final boolean defaultValue)
    {
        return get(key, defaultValue);
    }

    /**
     * Gets the value of the given key as an ObjectId.
     *
     * @param key the key
     * @return the value as an ObjectId, which may be null
     * @throws java.lang.ClassCastException if the value is not an ObjectId
     */
    public ObjectId getObjectId(final Object key)
    {
        return (ObjectId) get(key);
    }

    /**
     * Gets the value of the given key as a Date.
     *
     * @param key the key
     * @return the value as a Date, which may be null
     * @throws java.lang.ClassCastException if the value is not a Date
     */
    public Date getDate(final Object key)
    {
        return (Date) get(key);
    }

    /**
     * Gets the list value of the given key, casting the list elements to the given {@code Class<T>}.  This is useful to avoid having
     * casts in client code, though the effect is the same.
     *
     * @param key   the key
     * @param clazz the non-null class to cast the list value to
     * @param <T>   the type of the class
     * @return the list value of the given key, or null if the instance does not contain this key.
     * @throws ClassCastException if the elements in the list value of the given key is not of type T or the value is not a list
     * @since 3.10
     */
    public <T> List<T> getList(final Object key, final Class<T> clazz)
    {
        notNull("clazz", clazz);
        return constructValuesList(key, clazz, null);
    }

    /**
     * Gets the list value of the given key, casting the list elements to {@code Class<T>} or returning the default list value if null.
     * This is useful to avoid having casts in client code, though the effect is the same.
     *
     * @param key   the key
     * @param clazz the non-null class to cast the list value to
     * @param defaultValue what to return if the value is null
     * @param <T>   the type of the class
     * @return the list value of the given key, or the default list value if the instance does not contain this key.
     * @throws ClassCastException if the value of the given key is not of type T
     * @since 3.10
     */
    public <T> List<T> getList(final Object key, final Class<T> clazz, final List<T> defaultValue)
    {
        notNull("defaultValue", defaultValue);
        notNull("clazz", clazz);
        return constructValuesList(key, clazz, defaultValue);
    }

    // Construct the list of values for the specified key, or return the default value if the value is null.
    // A ClassCastException will be thrown if an element in the list is not of type T.
    @SuppressWarnings("unchecked")
    private <T> List<T> constructValuesList(final Object key, final Class<T> clazz, final List<T> defaultValue)
    {
        List<?> value = get(key, List.class);
        if (value == null) {
            return defaultValue;
        }

        for (Object item : value) {
            if (!clazz.isAssignableFrom(item.getClass())) {
                throw new ClassCastException(format("List element cannot be cast to %s", clazz.getName()));
            }
        }
        return (List<T>) value;
    }

    /**
     * Gets a JSON representation of this document using the {@link org.bson.json.JsonMode#RELAXED} output mode, and otherwise the default
     * settings of {@link JsonWriterSettings.Builder} and {@link DocumentCodec}.
     *
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the document contains types not in the default registry
     * @see #toJson(JsonWriterSettings)
     * @see JsonWriterSettings
     */
    @SuppressWarnings("deprecation")
    public String toJson()
    {
        return toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build());
    }

    /**
     * Gets a JSON representation of this document
     *
     * <p>With the default {@link DocumentCodec}.</p>
     *
     * @param writerSettings the json writer settings to use when encoding
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the document contains types not in the default registry
     */
    public String toJson(final JsonWriterSettings writerSettings)
    {
        return toJson(writerSettings, new DocumentCodec());
    }

    /**
     * Gets a JSON representation of this document
     *
     * <p>With the default {@link JsonWriterSettings}.</p>
     *
     * @param encoder the document codec instance to use to encode the document
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the registry does not contain a codec for the document values.
     */
    @SuppressWarnings("deprecation")
    public String toJson(final Encoder<Document> encoder)
    {
        return toJson(JsonWriterSettings.builder().outputMode(JsonMode.RELAXED).build(), encoder);
    }

    /**
     * Gets a JSON representation of this document
     *
     * @param writerSettings the json writer settings to use when encoding
     * @param encoder the document codec instance to use to encode the document
     * @return a JSON representation of this document
     * @throws org.bson.codecs.configuration.CodecConfigurationException if the registry does not contain a codec for the document values.
     */
    public String toJson(final JsonWriterSettings writerSettings, final Encoder<Document> encoder)
    {
        JsonWriter writer = new JsonWriter(new StringWriter(), writerSettings);
        encoder.encode(writer, this, EncoderContext.builder().build());
        return writer.getWriter().toString();
    }

    /**
     * Put the given key/value pair into this Document and return this.  Useful for chaining puts in a single expression, e.g.
     * <pre>
     * doc.append("a", 1).append("b", 2)}
     * </pre>
     * @param key   key
     * @param value value
     * @return this
     */
    //Original append had String key
    public Document append(Object key, final Object value)
    {
        if(isJDocument()) {
            documentAsMapj.put((String) key, value);
            return this;
        }
        else {
            documentAsMapclj = documentAsMapclj.assoc(key, value);
            return this;
        }
    }

    /////////////////////////////////////////////////////////////////////////////////////////////////


    //MapEquivalence


    //IPersistentMap

    //3gets

    @Override
    public Object get(Object key)
    {
        if(isJDocument())
        {
            return documentAsMapj.get(key);
        }
        else
        {
            return ((Map) documentAsMapclj).get(key);
        }
    }

    public <T> T get(final Object key, final Class<T> clazz)
    {
        if(isJDocument())
        {
            notNull("clazz", clazz);
            return clazz.cast(documentAsMapj.get(key));
        }
        else
        {
            notNull("clazz", clazz);
            Object value = ((Map) documentAsMapclj).get(key);
            return clazz.cast(value);
        }
    }

    @SuppressWarnings("unchecked")
    public <T> T get(final Object key, final T defaultValue)
    {
        if(isJDocument())
        {
            notNull("defaultValue", defaultValue);
            Object value = documentAsMapj.get(key);
            return value == null ? defaultValue : (T) value;
        }
        else
        {
            notNull("defaultValue", defaultValue);
            Object value = ((Map) documentAsMapclj).get(key);
            return value == null ? defaultValue : (T) value;
        }
    }

    @Override
    public boolean equiv(Object o)
    {
        //System.out.println("Calling equiv()");

        return documentAsMapclj.equiv(o);
    }

    @Override
    public IMapEntry entryAt(Object o)
    {
        //System.out.println("Calling entryAt()");
        return documentAsMapclj.entryAt(o);
    }

    @Override
    public Object valAt(Object o)
    {
        //System.out.println("Calling valAt()");
        return documentAsMapclj.valAt(o);
    }

    @Override
    public Object valAt(Object o, Object o1)
    {
        //System.out.println("Calling valAt()");
        return documentAsMapclj.valAt(o,o1);
    }

    @Override
    public int count()
    {
        //System.out.println("Calling count()");
        return documentAsMapclj.count();
    }

    @Override
    public IPersistentCollection empty()
     {
        //System.out.println("Calling empty()");
        //documentAsMapclj=(IPersistentMap) documentAsMapclj.empty();
          return new Document();
        }

    @Override
    public IPersistentCollection cons(Object o)
    {
        //System.out.println("Calling cons()");
        //documentAsMapclj=(IPersistentMap) documentAsMapclj.cons(o);
         return new Document(true,(Map) documentAsMapclj.cons(o));
    }

    @Override
    public IPersistentMap assoc(Object o, Object o1)
    {
        //System.out.println("Calling assoc()");
        //documentAsMapclj=
       return new Document(true,(Map) documentAsMapclj.assoc(o,o1));
    }

    @Override
    public IPersistentMap without(Object o)
    {
        //System.out.println("Calling without()");
        //documentAsMapclj= (IPersistentMap) documentAsMapclj.without(o);
      return new Document(true,(Map) documentAsMapclj.without(o));
    }

    // bug document shouldnt have a seq,so when document.seq a superclass would take it

    @Override
    public ISeq seq()
    {
        //System.out.println("Calling seq()");

        if(isJDocument())
        {
            return (ISeq) seqC.invoke(documentAsMapj);
        }
        else
        {
            return documentAsMapclj.seq();
        }
    }

    @Override
    public IPersistentMap assocEx(Object o, Object o1)
    {
        //System.out.println("Calling assocEx()");
      return new Document(true,(Map) documentAsMapclj.assocEx(o,o1));
    }

    @Override
    public Set<Map.Entry<String, Object>> entrySet()
    {
        if(isJDocument())
        {
            return documentAsMapj.entrySet();
        }
        else
        {
            return ((Map) documentAsMapclj).entrySet();
        }
    }

    @Override
    public Iterator iterator()
    {
        return documentAsMapclj.iterator();
    }

    //IFN    TODO

    //Map
    @Override
    public int size()
    {
        if(isJDocument())
        {
            return documentAsMapj.size();
        }
        else
        {
            return ((Map) documentAsMapclj).size();
        }
    }

    @Override
    public boolean isEmpty()
    {
        if(isJDocument())
        {
            return documentAsMapj.isEmpty();
        }
        else
        {
            return ((Map) documentAsMapclj).isEmpty();
        }
    }

    @Override
    public Set keySet()
    {
        if(isJDocument())
        {
            return documentAsMapj.keySet();
        }
        else
        {
            return ((Map) documentAsMapclj).keySet();
        }
    }

    @Override
    public boolean containsValue(Object value)
    {
        if(isJDocument())
        {
            return documentAsMapj.containsValue(value);
        }
        else
        {
            return ((Map) documentAsMapclj).containsValue(value);
        }
    }

    @Override
    public Collection values()
    {
        if(isJDocument())
        {
            return documentAsMapj.values();
        }
        else
        {
            return ((Map) documentAsMapclj).values();
        }
    }

    @Override
    public void forEach(Consumer action)
    {
        //System.out.println("Calling forEach()");
        documentAsMapclj.forEach(action);
    }

    @Override
    public Spliterator spliterator()
    {
        //System.out.println("Calling spliterator()");
        return documentAsMapclj.spliterator();
    }


    //Map(extra not in ordered-map)

    @Override
    public void clear()
    {
        if(isJDocument())
        {
            documentAsMapj.clear();
        }
        else
        {
            documentAsMapclj = (IPersistentMap) PersistentArrayMap.create(new HashMap()); //createDocumentAsMapC.invoke();
        }
    }

    @Override
    public Object put(Object key, Object value)
    {
        if(isJDocument())
        {
            return documentAsMapj.put((String) key, value);
        }
        else
        {
            documentAsMapclj=documentAsMapclj.assoc(key,value);
            return value;
        }
    }

    @Override
    public void putAll(Map map)
    {
        if(isJDocument())
        {
            documentAsMapj.putAll(map);
        }
        else
        {
            documentAsMapclj = (IPersistentMap) mergeC.invoke(documentAsMapclj,map);
        }
    }

    @Override
    public boolean remove(Object key, Object value)
    {
        //System.out.println("Calling remove()");
        //TODO i remove it anyways
        documentAsMapclj = (IPersistentMap) dissocC.invoke(key);
        return true;
    }

    @Override
    public Object remove(Object key)
    {
        if(isJDocument())
        {
            return documentAsMapj.remove(key);
        }
        else
        {
            documentAsMapclj = (IPersistentMap) dissocC.invoke(key);
            return true;
        }
    }

    @Override
    public boolean containsKey(Object key)
    {
        if(isJDocument())
        {
            return documentAsMapj.containsKey(key);
        }
        else
        {
            return documentAsMapclj.containsKey(key);
        }
    }

    //Object

    @Override
    public String toString()
    {
        if(isJDocument())
        {
            return "Document{"
                    + documentAsMapj
                    + '}';
        }
        else
        {
            return documentAsMapclj.toString();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if(isJDocument())
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Document document = (Document) o;

            if (!documentAsMapj.equals(document.documentAsMapj)) {
                return false;
            }

            return true;
        }
        else
        {
            return documentAsMapclj.equals(o);
        }
    }

    @Override
    public int hashCode()
    {
        if(isJDocument())
        {
            return documentAsMapj.hashCode();
        }
        else
        {
            return documentAsMapclj.hashCode();
        }
    }

    /*TODO
    @Override
    public int hasheq() {
        return ((IHashEq) documentAsMapclj.);
    }

    */

    //@Override
    public IPersistentMap meta()
    {
        //System.out.println("Calling meta()");
        return ((IObj) documentAsMapclj).meta();
    }

    //@Override
    public IObj withMeta(IPersistentMap iPersistentMap)
    {
        //System.out.println("Calling withMeta()");
        return ((IObj) documentAsMapclj).withMeta(iPersistentMap);
    }

    //@Override
    public ITransientCollection asTransient()
    {
        //System.out.println("Calling asTransient()");
        return ((IEditableCollection) documentAsMapclj).asTransient();
    }

    //@Override
    public ISeq rseq()
    {
        //System.out.println("Calling rseq()");
        return ((Reversible) documentAsMapclj).rseq();
    }
}
