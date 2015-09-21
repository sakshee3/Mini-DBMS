import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
 
public class LRUCache {
 
	private final int capacity;
	private ConcurrentLinkedQueue queue;
	private ConcurrentHashMap map;
 
	public LRUCache(final int capacity) {
		this.capacity = capacity;
		this.queue = new ConcurrentLinkedQueue();
		this.map = new ConcurrentHashMap(capacity);

	}
 
	public String get(final String key) {
		return (String)map.get(key);
	}
 

	public void put(final String key, final String value) {
		if(key == null || value == null) {
			throw new NullPointerException();
		}
		if (map.containsKey(key)) {
			queue.remove(key);
		}
		while (queue.size() >= capacity) {
			String expiredKey = (String)queue.poll();
			if (expiredKey != null) {
				map.remove(expiredKey);
			}
		}
		queue.add(key);
		map.put(key, value);
	}

	/*public void invalidate(final String key){
		if (map.containsKey(key)) {
			queue.remove(key);
			map.remove(key);
		}
	}*/
} 

