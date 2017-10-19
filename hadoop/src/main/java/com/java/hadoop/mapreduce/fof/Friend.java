package com.java.hadoop.mapreduce.fof;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sgr on 2017/10/19/019.
 */
public class Friend implements WritableComparable<Friend> {

    private String friend1;
    private String friend2;
    private int hot;//亲密度

    public String getFriend1() {
        return friend1;
    }

    public void setFriend1(String friend1) {
        this.friend1 = friend1;
    }

    public String getFriend2() {
        return friend2;
    }

    public void setFriend2(String friend2) {
        this.friend2 = friend2;
    }

    public int getHot() {
        return hot;
    }

    public void setHot(int hot) {
        this.hot = hot;
    }

    /**
     * @param o the object to be compared.
     * @return a negative integer, zero, or a positive integer as this object
     * is less than, equal to, or greater than the specified object.
     * @throws NullPointerException if the specified object is null
     * @throws ClassCastException   if the specified object's type prevents it
     *                              from being compared to this object.
     */
    @Override
    public int compareTo(Friend o) {
        int c = this.friend1.compareTo(friend2);
        if (c == 0){
            return Integer.compare(this.hot,o.getHot());
        }
        return c;
    }

    /**
     * Serialize the fields of this object to <code>out</code>.
     *
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(friend1);
        out.writeUTF(friend2);
        out.writeInt(hot);
    }

    /**
     * Deserialize the fields of this object from <code>in</code>.
     * <p>
     * <p>For efficiency, implementations should attempt to re-use storage in the
     * existing object where possible.</p>
     *
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.friend1 = in.readUTF();
        this.friend2 = in.readUTF();
        this.hot = in.readInt();
    }
}
