package statelessOperator;

import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

import operator.merger.Merger;
import operator.merger.MergerThreadSafe;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MergerFactory {

	static private HashMap<String, MergerThreadSafe> mergers = new HashMap<String, MergerThreadSafe>();
	public static Logger LOG = LoggerFactory.getLogger(MergerFactory.class);
	static private ReentrantLock lock = new ReentrantLock();

	private MergerFactory() {
	}

	public static Merger boltFactory(String id, String applicant,
			List<String> ids) {
		lock.lock();
		LOG.info("Merger - boltFactory, lock aquired by " + applicant
				+ " to get merger for id " + id);
		if (!mergers.containsKey(id)) {
			LOG.info("Merger - boltFactory, merger does not exist, creating it");
			mergers.put(id, new MergerThreadSafe(id));
		}
		LOG.info("Merger - boltFactory, registering applicant");
		for (String s : ids) {
			LOG.info("Merger - boltFactory, registering " + s);
			mergers.get(id).registerInput(s);
		}
		LOG.info("Merger - boltFactory, lock is going to be released by "
				+ applicant);
		lock.unlock();
		return mergers.get(id);
	}

	public static Merger spoutFactory(String id, String applicant) {

		lock.lock();
		LOG.info("Merger - boltFactory, lock aquired by " + applicant
				+ " to get merger for id " + id);
		if (!mergers.containsKey(id)) {
			LOG.info("Merger - boltFactory, merger does not exist, creating it");
			mergers.put(id, new MergerThreadSafe(id));
		}
		LOG.info("Merger - boltFactory, lock is going to be released by "
				+ applicant);
		lock.unlock();
		return mergers.get(id);

	}

}
