package hw4;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

public class LockManager {

	private final ConcurrentMap<Integer, Object> locks;	// page id, object;
	private final Map<Integer, List<Integer>> sharedLocks;	// page id, transaction id
	private final Map<Integer, Integer> exclusiveLocks;	// page id, transaction id
	private final ConcurrentMap<Integer, Collection<Integer>> pageIdsLockedByTransaction;	// transaction id, page id
	private final ConcurrentMap<Integer, Collection<Integer>> dependencyGraph;	// transaction id, transaction id

	private LockManager() {
		locks = new ConcurrentHashMap<Integer, Object>();
		sharedLocks = new HashMap<Integer, List<Integer>>();
		exclusiveLocks = new HashMap<Integer, Integer>();
		pageIdsLockedByTransaction = new ConcurrentHashMap<Integer, Collection<Integer>>();
		dependencyGraph = new ConcurrentHashMap<Integer, Collection<Integer>>();
	}

	public static LockManager create() {
		return new LockManager();
	}

	private Object getLock(Integer pageId) {
		locks.putIfAbsent(pageId, new Object());
		return locks.get(pageId);
	}

	public boolean acquireLock(Integer transactionId, Integer pageId, Permissions permissions)
			throws Exception {
		Integer notNullTransactionId = (transactionId == -1) ? -1 : transactionId;
		if (permissions == Permissions.READ_ONLY) {
			if (hasReadPermissions(notNullTransactionId, pageId)) {
				return true;
			}
			while (!acquireReadOnlyLock(notNullTransactionId, pageId)) {
				// waiting for lock
			}
		} else if (permissions == Permissions.READ_WRITE) {
			if (hasWritePermissions(notNullTransactionId, pageId)) {
				return true;
			}
			while (!acquireReadWriteLock(notNullTransactionId, pageId)) {
				// waiting for lock
			}
		} else {
			throw new IllegalArgumentException("Expected either READ_ONLY or READ_WRITE permissions.");
		}
		addPageToTransactionLocks(notNullTransactionId, pageId);
		return true;
	}

	private boolean hasReadPermissions(Integer transactionId, Integer pageId) {
		if (hasWritePermissions(transactionId, pageId)) {
			return true;
		}
		return sharedLocks.containsKey(pageId) && sharedLocks.get(pageId).contains(transactionId);
	}

	private boolean hasWritePermissions(Integer transactionId, Integer pageId) {
		return exclusiveLocks.containsKey(pageId) && transactionId.equals(exclusiveLocks.get(pageId));
	}

	private void addPageToTransactionLocks(Integer transactionId, Integer pageId) {
		pageIdsLockedByTransaction.putIfAbsent(transactionId, new LinkedBlockingQueue<Integer>());
		pageIdsLockedByTransaction.get(transactionId).add(pageId);
	}

	public boolean acquireReadOnlyLock(Integer transactionId, Integer pageId)
			throws Exception {
		Object lock = getLock(pageId);
		while (true) {
			synchronized (lock) {
				Integer exclusiveLockHolder = exclusiveLocks.get(pageId);
				if (exclusiveLockHolder == null || transactionId.equals(exclusiveLockHolder)) {
					removeDependencies(transactionId);
					addSharedUser(transactionId, pageId);
					return true;
				}
				addDependency(transactionId, exclusiveLockHolder);
			}
		}
	}

	private void removeDependencies(Integer dependent) {
		dependencyGraph.remove(dependent);
	}

	private void addDependency(Integer dependent, Integer dependee)
			throws Exception {
		Collection<Integer> dependees = new ArrayList<Integer>();
		dependees.add(dependee);
		addDependencies(dependent, dependees);
	}

	private void addDependencies(Integer dependent, Collection<Integer> dependees)
			throws Exception {
		dependencyGraph.putIfAbsent(dependent, new LinkedBlockingQueue<Integer>());
		Collection<Integer> dependeesCollection = dependencyGraph.get(dependent);
		boolean addedDependee = false;
		for (Integer newDependee : dependees) {
			if (!dependeesCollection.contains(newDependee) && !newDependee.equals(dependent)) {
				addedDependee = true;
				dependeesCollection.add(newDependee);
			}
		}
		if (addedDependee) {
			abortIfDeadlocked();
		}
	}

	private void abortIfDeadlocked() throws Exception {
		Set<Integer> visitedTransactionIds = new HashSet<Integer>();
		for (Integer transactionId : dependencyGraph.keySet()) {
			if (!visitedTransactionIds.contains(transactionId)) {
				testForDeadlock(transactionId, visitedTransactionIds, new Stack<Integer>());
			}
		}
	}

	private void testForDeadlock(Integer transactionId,Set<Integer> visitedTransactionIds, Stack<Integer> parents)
			throws Exception {
		visitedTransactionIds.add(transactionId);
		if (!dependencyGraph.containsKey(transactionId)) {
			return;
		}
		for (Integer dependee : dependencyGraph.get(transactionId)) {
			if (parents.contains(dependee)) {
				throw new Exception();
			}
			if (!visitedTransactionIds.contains(dependee)) {
				parents.push(transactionId);
				testForDeadlock(dependee, visitedTransactionIds, parents);
				parents.pop();
			}
		}
	}

	private void addSharedUser(Integer transactionId, Integer pageId) {
		if (!sharedLocks.containsKey(pageId)) {
			sharedLocks.put(pageId, new ArrayList<Integer>());
		}
		sharedLocks.get(pageId).add(transactionId);
	}

	private Collection<Integer> getLockHolders(Integer pageId) {
		Collection<Integer> lockHolders = new ArrayList<Integer>();
		if (exclusiveLocks.containsKey(pageId)) {
			lockHolders.add(exclusiveLocks.get(pageId));
			return lockHolders;
		}
		if (sharedLocks.containsKey(pageId)) {
			lockHolders.addAll(sharedLocks.get(pageId));
		}
		return lockHolders;
	}

	private boolean isLockedByOthers(Integer transactionId, Collection<Integer> lockHolders) {
		if (lockHolders == null || lockHolders.isEmpty()) {
			return false;
		}
		if (lockHolders.size() == 1 && transactionId.equals(lockHolders.iterator().next())) {
			return false;
		}
		return true;
	}

	private void addExclusiveUser(Integer transactionId, Integer pageId) {
		exclusiveLocks.put(pageId, transactionId);
	}

	public boolean acquireReadWriteLock(Integer transactionId, Integer pageId)
			throws Exception {
		Object lock = getLock(pageId);
		while (true) {
			synchronized (lock) {
				Collection<Integer> lockHolders = getLockHolders(pageId);
				if (!isLockedByOthers(transactionId, lockHolders)) {
					removeDependencies(transactionId);
					addExclusiveUser(transactionId, pageId);
					return true;
				}
				addDependencies(transactionId, lockHolders);
			}
		}
	}

	private void releaseLock(Integer transactionId, Integer pageId) {
		Object lock = getLock(pageId);
		synchronized (lock) {
			exclusiveLocks.remove(pageId);
			if (sharedLocks.containsKey(pageId)) {
				sharedLocks.get(pageId).remove(transactionId);
			}
		}
	}

	public void releasePage(Integer transactionId, Integer pageId) {
		releaseLock(transactionId, pageId);
		if (pageIdsLockedByTransaction.containsKey(transactionId)) {
			pageIdsLockedByTransaction.get(transactionId).remove(pageId);
		}
	}

	public void releasePages(Integer transactionId) {
		if (pageIdsLockedByTransaction.containsKey(transactionId)) {
			Collection<Integer> pageIds = pageIdsLockedByTransaction.get(transactionId);
			for (Integer pageId : pageIds) {
				releaseLock(transactionId, pageId);
			}
			pageIdsLockedByTransaction.replace(transactionId, new LinkedBlockingQueue<Integer>());
		}
	}

	public boolean holdsLock(Integer transactionId, Integer pageId) {
		if (!pageIdsLockedByTransaction.containsKey(transactionId)) {
			return false;
		}
		return pageIdsLockedByTransaction.get(transactionId).contains(pageId);
	}
}