import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

// ordered, support quick merge and random choice
public class MembershipList implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private static class Member implements Serializable {
        private static final long serialVersionUID = 1L;

        final Address address;
        int heartbeatCounter;
        transient long lastUpdateTime;
        boolean leave;

        public static final Comparator<Member> compareByAddress = new Comparator<Member>() {
            @Override
            public int compare(Member o1, Member o2) {
                return o1.address.compareTo(o2.address);
            }
        };

        Member(Address address) {
            this(address, 0, 0, false);
        }

        Member(Address address, int heartbeatCounter, long lastUpdateTime, boolean leave) {
            this.address = address;
            this.heartbeatCounter = heartbeatCounter;
            this.lastUpdateTime = lastUpdateTime;
            this.leave = leave;
        }
    }

    private List<Member> membershipList;
    private transient final Address selfAddress;

    private MembershipList(Address selfAddress) {
        membershipList = new ArrayList<>();
        this.selfAddress = selfAddress;
    }

    public static MembershipList createMembershipList(Address selfAddress, Address... initialAdds) {
        MembershipList ml = new MembershipList(selfAddress);

        long currentTime = System.currentTimeMillis();
        ml.membershipList.add(new Member(selfAddress, 0, currentTime, false));
        for (Address address : initialAdds) {
            if (!address.equals(selfAddress)) {
                ml.membershipList.add(new Member(address, 0, currentTime, false));
            }
        }
        Collections.sort(ml.membershipList, Member.compareByAddress);

        return ml;
    }

    public synchronized void merge(MembershipList msl) {
        long currentTime = System.currentTimeMillis();

        int i = 0;
        int j = 0;
        List<Member> newList = new ArrayList<>();
        while (i < size() && j < msl.size()) {
            Member m1 = membershipList.get(i);
            Member m2 = msl.membershipList.get(j);

            int cp = Member.compareByAddress.compare(m1, m2);
            if (cp == 0) {
                i++;
                j++;
                if (m2.heartbeatCounter > m1.heartbeatCounter) {
                    m1.heartbeatCounter = m2.heartbeatCounter;
                    m1.lastUpdateTime = currentTime;
                    m1.leave = m2.leave;
                }
                newList.add(m1);
            } else if (cp < 0) {
                i++;
                newList.add(m1);
            } else {
                newList.add(new Member(m2.address, m2.heartbeatCounter, currentTime, false));
                j++;
            }
        }

        while (i < size()) {
            newList.add(membershipList.get(i++));
        }

        while (j < msl.size()) {
            Member m = msl.membershipList.get(j++);
            newList.add(new Member(m.address, m.heartbeatCounter, currentTime, m.leave));
        }

        membershipList = newList;
    }

    public synchronized boolean isAlive(Address address) {
        int idx = Collections.binarySearch(membershipList, new Member(address),
                Member.compareByAddress);
        return idx >= 0
                && System.currentTimeMillis()
                        - membershipList.get(idx).lastUpdateTime <= Catalog.FAIL_TIME
                && !membershipList.get(idx).leave;
    }

    // Except self
    public synchronized List<Address> getAliveMembersExceptSelf() {
        long currentTime = System.currentTimeMillis();
        List<Address> list = new ArrayList<>();
        for (Member m : membershipList) {
            if (!m.address.equals(selfAddress)
                    && currentTime - m.lastUpdateTime <= Catalog.FAIL_TIME && !m.leave) {
                list.add(m.address);
            }
        }
        return list;
    }

    // Include self. increment self heartbeatCounter, cleanup, and get alive
    // sublist
    public synchronized MembershipList updateAndGetNonFailMembers() {
        MembershipList sublist = new MembershipList(selfAddress);
        List<Member> newList = new ArrayList<>();

        long currentTime = System.currentTimeMillis();
        for (Member m : membershipList) {
            if (m.address.equals(selfAddress)) {
                m.heartbeatCounter += 1;
                m.lastUpdateTime = currentTime;
            }

            long t = currentTime - m.lastUpdateTime;
            if (t <= Catalog.CLEANUP_TIME) {
                newList.add(m);
                if (t <= Catalog.FAIL_TIME) {
                    sublist.membershipList.add(m);
                }
            }
        }

        membershipList = newList;
        return sublist;
    }

    public synchronized Address getRandomAliveMember() {
        List<Address> aliveMembers = getAliveMembersExceptSelf();
        if (aliveMembers.size() == 0) {
            return null;
        } else {
            int k = ThreadLocalRandom.current().nextInt(aliveMembers.size());
            return aliveMembers.get(k);
        }
    }

    public synchronized MembershipList voluntaryLeaveMessage() {
        int idx = Collections.binarySearch(membershipList, new Member(selfAddress),
                Member.compareByAddress);
        Member self = membershipList.get(idx);
        MembershipList vlm = new MembershipList(self.address);
        vlm.membershipList.add(new Member(self.address, self.heartbeatCounter + 1,
                System.currentTimeMillis(), true));
        return vlm;
    }

    public synchronized int size() {
        return membershipList.size();
    }

    @Override
    public synchronized String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format("{%n"));
        for (Member m : membershipList) {
            sb.append(String.format("\t%s\t%s\t%s\t%s%n", m.address, m.heartbeatCounter,
                    m.lastUpdateTime, m.leave));
        }
        sb.append("}");
        return sb.toString();
    }
}
