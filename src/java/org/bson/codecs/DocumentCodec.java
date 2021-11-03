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

package org.bson.codecs;

import clojure.java.api.Clojure;
import clojure.lang.*;
import org.bson.BsonDocument;
import org.bson.BsonDocumentWriter;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.BsonValue;
import org.bson.BsonWriter;
import org.bson.Document;
import org.bson.Transformer;
import org.bson.UuidRepresentation;
import org.bson.codecs.configuration.CodecRegistry;

import java.util.*;
import static java.util.Arrays.asList;
import static org.bson.assertions.Assertions.notNull;
import static org.bson.codecs.BsonTypeClassMap.DEFAULT_BSON_TYPE_CLASS_MAP;
import static org.bson.codecs.configuration.CodecRegistries.fromProviders;

//WRITER,ENCODECONTEXT=>ENCODE

/**
 * A Codec for Document instances.
 *
 * @see org.bson.Document
 * @since 3.0
 */
public class DocumentCodec implements CollectibleCodec<Document>, OverridableUuidRepresentationCodec<Document>
{
    //0 = do what static says,1=java,2=clojure
    private boolean javaDecode=true;

    public void setJavaDecode(boolean isjavaDocument)
    {
        this.javaDecode=isjavaDocument;
    }

    public boolean isJavaDecode()
    {
        return javaDecode;
    }

    private static final String ID_FIELD_NAME = "_id";
    private static final CodecRegistry DEFAULT_REGISTRY = fromProviders(asList(new ValueCodecProvider(),
            new BsonValueCodecProvider(),
            new DocumentCodecProvider()));
    private static final BsonTypeCodecMap DEFAULT_BSON_TYPE_CODEC_MAP = new BsonTypeCodecMap(DEFAULT_BSON_TYPE_CLASS_MAP, DEFAULT_REGISTRY);
    private static final IdGenerator DEFAULT_ID_GENERATOR = new ObjectIdGenerator();

    private final BsonTypeCodecMap bsonTypeCodecMap;
    private final CodecRegistry registry;
    private final IdGenerator idGenerator;
    private final Transformer valueTransformer;
    private final UuidRepresentation uuidRepresentation;

    //ClojureOnly////////////////////////////////////////////////////////////
    static IFn isKeywordC = Clojure.var("clojure.core", "keyword?");
    //static IFn  keywordC = Clojure.var("clojure.core", "keyword");
    static IFn  nameC = Clojure.var("clojure.core", "name");
    static IFn  transientC = Clojure.var("clojure.core", "transient");
    static IFn  persistentC = Clojure.var("clojure.core", "persistent!");
    static IFn  conjTransientC = Clojure.var("clojure.core", "conj!");
    ///////////////////////////////////////////////////////////

    private boolean isJDocument(Document document)
    {
        return (document.documentAsMapj!=null);
    }

    private boolean isJList(Iterable<Object> list)
    {
        return !PersistentVector.class.isInstance(list);
    }

    /**
     * Construct a new instance with a default {@code CodecRegistry}.
     */
    public DocumentCodec() {
        this(DEFAULT_REGISTRY, DEFAULT_BSON_TYPE_CODEC_MAP, null);
    }

    /**
     * Construct a new instance with the given registry.
     *
     * @param registry         the registry
     * @since 3.5
     */
    public DocumentCodec(final CodecRegistry registry) {
        this(registry, DEFAULT_BSON_TYPE_CLASS_MAP);
    }

    /**
     * Construct a new instance with the given registry and BSON type class map.
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     */
    public DocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap) {
        this(registry, bsonTypeClassMap, null);
    }

    public DocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap,int clj)
    {
        this(registry, bsonTypeClassMap, null);
    }

    /**
     * Construct a new instance with the given registry and BSON type class map. The transformer is applied as a last step when decoding
     * values, which allows users of this codec to control the decoding process.  For example, a user of this class could substitute a
     * value decoded as a Document with an instance of a special purpose class (e.g., one representing a DBRef in MongoDB).
     *
     * @param registry         the registry
     * @param bsonTypeClassMap the BSON type class map
     * @param valueTransformer the value transformer to use as a final step when decoding the value of any field in the document
     */
    public DocumentCodec(final CodecRegistry registry, final BsonTypeClassMap bsonTypeClassMap, final Transformer valueTransformer) {
        this(registry, new BsonTypeCodecMap(notNull("bsonTypeClassMap", bsonTypeClassMap), registry), valueTransformer);
    }

    private DocumentCodec(final CodecRegistry registry, final BsonTypeCodecMap bsonTypeCodecMap, final Transformer valueTransformer) {
        this(registry, bsonTypeCodecMap, DEFAULT_ID_GENERATOR, valueTransformer, UuidRepresentation.UNSPECIFIED);
    }

    private DocumentCodec(final CodecRegistry registry, final BsonTypeCodecMap bsonTypeCodecMap, final IdGenerator idGenerator,
                          final Transformer valueTransformer, final UuidRepresentation uuidRepresentation) {
        this.registry = notNull("registry", registry);
        this.bsonTypeCodecMap = bsonTypeCodecMap;
        this.idGenerator = idGenerator;
        this.valueTransformer = valueTransformer != null ? valueTransformer : new Transformer() {
            @Override
            public Object transform(final Object value) {
                return value;
            }
        };
        this.uuidRepresentation = uuidRepresentation;
    }

    @Override
    public Codec<Document> withUuidRepresentation(final UuidRepresentation uuidRepresentation) {
        return new DocumentCodec(registry, bsonTypeCodecMap, idGenerator, valueTransformer, uuidRepresentation);
    }

    @Override
    public boolean documentHasId(final Document document) {
        return document.containsKey(ID_FIELD_NAME);
    }

    @Override
    public BsonValue getDocumentId(final Document document) {
        if (!documentHasId(document)) {
            throw new IllegalStateException("The document does not contain an _id");
        }

        Object id = document.get(ID_FIELD_NAME);
        if (id instanceof BsonValue) {
            return (BsonValue) id;
        }

        BsonDocument idHoldingDocument = new BsonDocument();
        BsonWriter writer = new BsonDocumentWriter(idHoldingDocument);
        writer.writeStartDocument();
        writer.writeName(ID_FIELD_NAME);
        if(isJDocument(document))
        {
            writeValuej(writer, EncoderContext.builder().build(), id);
        }
        else
        {
            writeValueclj(writer, EncoderContext.builder().build(), id);
        }

        writer.writeEndDocument();
        return idHoldingDocument.get(ID_FIELD_NAME);
    }

    @Override
    public Document generateIdIfAbsentFromDocument(final Document document) {
        if (!documentHasId(document)) {
            document.put(ID_FIELD_NAME, idGenerator.generate());
        }
        return document;
    }

    //Clojure///////////////////////////////////////////////////////////////////////////////
    @Override
    public void encode(final BsonWriter writer, final Document document, final EncoderContext encoderContext)
    {
        if(isJDocument(document))
        {
            writeMapj(writer, document, encoderContext);
        }
        else
        {
            writeMapclj(writer, document, encoderContext);
        }
    }

    //java version of decode
    private Document decodej(final BsonReader reader, final DecoderContext decoderContext) {
        Document document = new Document();

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            String fieldName = reader.readName();
            document.put(fieldName, readValue(reader, decoderContext));
        }

        reader.readEndDocument();

        return document;
    }

    private Document decodeclj(final BsonReader reader, final DecoderContext decoderContext)
    {
        //if PersistenArrayMap didnt work why?
        ITransientMap tm = (ITransientMap) PersistentArrayMap.EMPTY.asTransient(); //transientC.invoke(PersistentArrayMap.EMPTY);

        reader.readStartDocument();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT)
        {

            String fieldName = reader.readName();
            //Keyword kfieldName = (Keyword)  keywordC.invoke(fieldName);
            Keyword kfieldName = Keyword.intern(fieldName);
            tm=tm.assoc(kfieldName,readValue(reader, decoderContext));
        }

        reader.readEndDocument();

        IPersistentMap pm = (IPersistentMap) tm.persistent(); //persistentC.invoke(tm);
        Document document = new Document(true,(Map) pm);
        return document;
    }

    //TODO  i can take the reader,and read it my way and return a Document
    //also i need to check decoderContext to know what i want out
    @Override
    public Document decode(final BsonReader reader, final DecoderContext decoderContext)
    {
        Document document;
        if(isJavaDecode())
        {
            document= decodej(reader,decoderContext);
        }
        else
        {
            document= decodeclj(reader,decoderContext);
        }
        //System.out.println(document);
        return document;
    }

    @Override
    public Class<Document> getEncoderClass() {
        return Document.class;
    }

    private void beforeFieldsj(final BsonWriter bsonWriter, final EncoderContext encoderContext, final Map<String, Object> document) {
        if (encoderContext.isEncodingCollectibleDocument() && document.containsKey(ID_FIELD_NAME)) {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValuej(bsonWriter, encoderContext, document.get(ID_FIELD_NAME));

        }
    }

    //Clojure exactly like original just final changed Map<String, Object> document
    private void beforeFieldsclj(final BsonWriter bsonWriter, final EncoderContext encoderContext, final Map<Object, Object> document)
    {
        if (encoderContext.isEncodingCollectibleDocument() && document.containsKey(ID_FIELD_NAME))
        {
            bsonWriter.writeName(ID_FIELD_NAME);
            writeValueclj(bsonWriter, encoderContext, document.get(ID_FIELD_NAME));
        }
    }

    private boolean skipField(final EncoderContext encoderContext, final String key) {
        return encoderContext.isEncodingCollectibleDocument() && key.equals(ID_FIELD_NAME);
    }

    private void writeValuej(final BsonWriter writer, final EncoderContext encoderContext, final Object value)
    {
        //System.out.println("writeValuej" + (value instanceof Iterable));
        if (value == null) {
            writer.writeNull();
        }
        else if (value instanceof Map) {
            writeMapj(writer, (Map<String, Object>) value, encoderContext.getChildContext());
        }
        else if (value instanceof Iterable)
        {
            writeIterable(writer, (Iterable<Object>) value, encoderContext.getChildContext());
        }  else {
            Codec codec = registry.get(value.getClass());
            encoderContext.encodeWithChildContext(codec, writer, value);
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private void writeValueclj(final BsonWriter writer, final EncoderContext encoderContext, final Object value) {
        if (value == null) {
            writer.writeNull();
        }
        else if (value instanceof Map)   ////i changed the order,because my map is also Iterable
        {
            writeMapclj(writer, (Map<Object, Object>) value, encoderContext.getChildContext());
        }
        else if (value instanceof Iterable)  //this was first
        {
            writeIterable(writer, (Iterable<Object>) value, encoderContext.getChildContext());
        }
        else
        {
            Codec codec = registry.get(value.getClass());
            encoderContext.encodeWithChildContext(codec, writer, value);
        }
    }

    private void writeMapj(final BsonWriter writer, final Map<String, Object> map, final EncoderContext encoderContext) {
        writer.writeStartDocument();

        beforeFieldsj(writer, encoderContext, map);

        for (final Map.Entry<String, Object> entry : map.entrySet()) {
            if (skipField(encoderContext, entry.getKey())) {
                continue;
            }
            writer.writeName(entry.getKey());
            writeValuej(writer, encoderContext, entry.getValue());
        }
        writer.writeEndDocument();
    }

    private void writeMapclj(final BsonWriter writer, final Map<Object, Object> map, final EncoderContext encoderContext) {
        writer.writeStartDocument();

        beforeFieldsclj(writer, encoderContext, map);

        for (final Map.Entry<Object, Object> entry : map.entrySet())
        {
            Object ofieldName = entry.getKey();
            String fieldName;

            if((boolean) isKeywordC.invoke(ofieldName))
            {
                fieldName = (String) nameC.invoke(ofieldName);
            }
            else
            {
                fieldName = (String) ofieldName;
            }

            Object value = entry.getValue();

            if (skipField(encoderContext, (String) fieldName))
            {
                continue;
            }
            writer.writeName( (String) fieldName);
            writeValueclj(writer, encoderContext, value);

        }
        writer.writeEndDocument();
    }

    private void writeIterable(final BsonWriter writer, Iterable<Object> list, final EncoderContext encoderContext)
    {
        writer.writeStartArray();
        for (final Object value : list)
        {
            if(isJList(list))
            {
                writeValuej(writer, encoderContext, value);
            }
            else
            {
                writeValueclj(writer, encoderContext, value);
            }
        }
        writer.writeEndArray();
    }

    private Object readValue(final BsonReader reader, final DecoderContext decoderContext) {
        BsonType bsonType = reader.getCurrentBsonType();
        if (bsonType == BsonType.NULL) {
            reader.readNull();
            return null;
        } else if (bsonType == BsonType.ARRAY)
        {
            //Clojure//////////////////////////////////////////////////////////
            if(isJavaDecode())
            {
                return readListj(reader, decoderContext);
            }
            else
            {
                return readListclj(reader, decoderContext);
            }

        } else {
            Codec<?> codec = bsonTypeCodecMap.get(bsonType);

            if (bsonType == BsonType.BINARY && reader.peekBinarySize() == 16) {
                switch (reader.peekBinarySubType()) {
                    case 3:
                        if (uuidRepresentation == UuidRepresentation.JAVA_LEGACY
                                || uuidRepresentation == UuidRepresentation.C_SHARP_LEGACY
                                || uuidRepresentation == UuidRepresentation.PYTHON_LEGACY) {
                            codec = registry.get(UUID.class);
                        }
                        break;
                    case 4:
                        if (uuidRepresentation == UuidRepresentation.STANDARD) {
                            codec = registry.get(UUID.class);
                        }
                        break;
                    default:
                        break;
                }
            }
            return valueTransformer.transform(codec.decode(reader, decoderContext));
        }
    }

    private List<Object> readListj(final BsonReader reader, final DecoderContext decoderContext) {
        reader.readStartArray();
        List<Object> list = new ArrayList<Object>();
        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT) {
            list.add(readValue(reader, decoderContext));
        }
        reader.readEndArray();
        return list;
    }

    private List<Object> readListclj (final BsonReader reader, final DecoderContext decoderContext)
    {
        reader.readStartArray();

        ITransientVector tvector = (ITransientVector) PersistentVector.EMPTY.asTransient(); //transientC.invoke(PersistentVector.EMPTY);

        while (reader.readBsonType() != BsonType.END_OF_DOCUMENT)
        {
            tvector = (ITransientVector) tvector.conj(readValue(reader, decoderContext));
            //v=v.cons(readValue(reader, decoderContext));
        }
        reader.readEndArray();
        return (List) tvector.persistent(); //tvector.persistent();
    }
}