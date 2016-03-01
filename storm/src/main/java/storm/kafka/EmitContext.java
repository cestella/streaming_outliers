package storm.kafka;

import backtype.storm.task.TopologyContext;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.*;

public class EmitContext implements Cloneable,Serializable, Map<String, Object> {
    static final long serialVersionUID = 0xDEADBEEFL;

    public enum Type{
         MESSAGE_ID(PartitionManager.KafkaMessageId.class)
        ,STREAM_ID(String.class)
        ,TASK_ID(Integer.class)
        ,UUID(String.class)
        ,SPOUT_CONFIG(SpoutConfig.class)
        ,OPEN_CONFIG(Map.class)
        ,TOPOLOGY_CONTEXT(TopologyContext.class)
        ;
        Class<?> clazz;
        Type(Class<?> clazz) {
            this.clazz=  clazz;
        }

        public Class<?> clazz() {
           return clazz;
        }
    }
    public EmitContext() {
        this(new EnumMap<>(Type.class));
    }
    public EmitContext(EnumMap<Type, Object> context) {
        _context = context;
    }
    private EnumMap<Type, Object> _context;

    public <T> EmitContext with(Type t, T o ) {
        _context.put(t, t.clazz().cast(o));
        return this;
    }
    public <T> void add(Type t, T o ) {
        with(t, o);
    }

    public <T> T get(Type t) {
        Object o = _context.get(t);
        if(o == null) {
            return null;
        }
        else {
            return (T) o;
        }
    }

    public EmitContext cloneContext() {
        try {
            return (EmitContext)this.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException("Unable to clone emit context.", e);
        }
    }

    private Type stringToKey(Object k) {
        return Type.valueOf(k.toString());
    }

    /**
     * Returns the number of key-value mappings in this map.
     *
     * @return the number of key-value mappings in this map
     */
    @Override
    public int size() {
        return _context.size();
    }

    /**
     * Returns <tt>true</tt> if this map maps one or more keys to the
     * specified value.
     *
     * @param value the value whose presence in this map is to be tested
     * @return <tt>true</tt> if this map maps one or more keys to this value
     */
    @Override
    public boolean containsValue(Object value) {
        return _context.containsValue(value);
    }

    /**
     * Returns <tt>true</tt> if this map contains a mapping for the specified
     * key.
     *
     * @param key the key whose presence in this map is to be tested
     * @return <tt>true</tt> if this map contains a mapping for the specified
     *            key
     */
    @Override
    public boolean containsKey(Object key) {
        return _context.containsKey(stringToKey(key));
    }

    /**
     * Returns the value to which the specified key is mapped,
     * or {@code null} if this map contains no mapping for the key.
     *
     * <p>More formally, if this map contains a mapping from a key
     * {@code k} to a value {@code v} such that {@code (key == k)},
     * then this method returns {@code v}; otherwise it returns
     * {@code null}.  (There can be at most one such mapping.)
     *
     * <p>A return value of {@code null} does not <i>necessarily</i>
     * indicate that the map contains no mapping for the key; it's also
     * possible that the map explicitly maps the key to {@code null}.
     * The {@link #containsKey containsKey} operation may be used to
     * distinguish these two cases.
     * @param key
     */
    @Override
    public Object get(Object key) {
        return _context.get(stringToKey(key));
    }

    /**
     * Associates the specified value with the specified key in this map.
     * If the map previously contained a mapping for this key, the old
     * value is replaced.
     *
     * @param key the key with which the specified value is to be associated
     * @param value the value to be associated with the specified key
     *
     * @return the previous value associated with specified key, or
     *     <tt>null</tt> if there was no mapping for key.  (A <tt>null</tt>
     *     return can also indicate that the map previously associated
     *     <tt>null</tt> with the specified key.)
     * @throws NullPointerException if the specified key is null
     */
    public Object put(String key, Object value) {
        return _context.put(stringToKey(key), value);
    }

    /**
     * Removes the mapping for this key from this map if present.
     *
     * @param key the key whose mapping is to be removed from the map
     * @return the previous value associated with specified key, or
     *     <tt>null</tt> if there was no entry for key.  (A <tt>null</tt>
     *     return can also indicate that the map previously associated
     *     <tt>null</tt> with the specified key.)
     */
    @Override
    public Object remove(Object key) {
        return _context.remove(stringToKey(key));
    }

    /**
     * Copies all of the mappings from the specified map to this map.
     * These mappings will replace any mappings that this map had for
     * any of the keys currently in the specified map.
     *
     * @param m the mappings to be stored in this map
     * @throws NullPointerException the specified map is null, or if
     *     one or more keys in the specified map are null
     */
    public void putAll(Map<? extends String, ?> m) {
        for(Map.Entry<? extends String, ?> kv : m.entrySet()) {
            _context.put(stringToKey(kv.getKey()), kv.getValue());
        }
    }

    /**
     * Removes all mappings from this map.
     */
    @Override
    public void clear() {
        _context.clear();
    }

    /**
     * Returns a {@link Set} view of the keys contained in this map.
     * The returned set obeys the general contract outlined in
     * {@link Map#keySet()}.  The set's iterator will return the keys
     * in their natural order (the order in which the enum constants
     * are declared).
     *
     * @return a set view of the keys contained in this enum map
     */
    @Override
    public Set<String> keySet() {
        return Sets.newHashSet(Iterables.transform(_context.keySet(), Functions.toStringFunction()));
    }

    /**
     * Returns a {@link Collection} view of the values contained in this map.
     * The returned collection obeys the general contract outlined in
     * {@link Map#values()}.  The collection's iterator will return the
     * values in the order their corresponding keys appear in map,
     * which is their natural order (the order in which the enum constants
     * are declared).
     *
     * @return a collection view of the values contained in this map
     */
    @Override
    public Collection<Object> values() {
        return _context.values();
    }

    /**
     * Returns a {@link Set} view of the mappings contained in this map.
     * The returned set obeys the general contract outlined in
     * {@link Map#keySet()}.  The set's iterator will return the
     * mappings in the order their keys appear in map, which is their
     * natural order (the order in which the enum constants are declared).
     *
     * @return a set view of the mappings contained in this enum map
     */
    @Override
    public Set<Entry<String, Object>> entrySet() {
        return Sets.newHashSet(Iterables.transform(_context.entrySet(), new Function<Entry<Type, Object>, Entry<String, Object>>() {
            @Nullable
            @Override
            public Entry<String, Object> apply(@Nullable Entry<Type, Object> typeObjectEntry) {
                return new AbstractMap.SimpleEntry<>(typeObjectEntry.getKey().toString(), typeObjectEntry.getValue());
            }
        }));
    }

    /**
     * {@inheritDoc}
     *
     * <p>This implementation returns <tt>size() == 0</tt>.
     */
    @Override
    public boolean isEmpty() {
        return _context.isEmpty();
    }

    /**
     * Returns a string representation of this map.  The string representation
     * consists of a list of key-value mappings in the order returned by the
     * map's <tt>entrySet</tt> view's iterator, enclosed in braces
     * (<tt>"{}"</tt>).  Adjacent mappings are separated by the characters
     * <tt>", "</tt> (comma and space).  Each key-value mapping is rendered as
     * the key followed by an equals sign (<tt>"="</tt>) followed by the
     * associated value.  Keys and values are converted to strings as by
     * {@link String#valueOf(Object)}.
     *
     * @return a string representation of this map
     */
    @Override
    public String toString() {
        return _context.toString();
    }

    @Override
    protected Object clone() throws CloneNotSupportedException {
        EmitContext context = new EmitContext(_context.clone());
        return context;
    }
}
