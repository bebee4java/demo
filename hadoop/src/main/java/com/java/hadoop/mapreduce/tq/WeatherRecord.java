package com.java.hadoop.mapreduce.tq;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by sgr on 2017/10/18/018.
 */
public class WeatherRecord implements WritableComparable<WeatherRecord> {

    private int year;
    private int month;
    private int day;
    private double wd;//温度

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public double getWd() {
        return wd;
    }

    public void setWd(double wd) {
        this.wd = wd;
    }

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
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
    public int compareTo(WeatherRecord o) {
        int c1 = Integer.compare(this.year, o.getYear());
        if (c1 == 0){
            //年份相同
            int c2 = Integer.compare(this.month,o.getMonth());
            if (c2 == 0){
                //月份相同
                return Double.compare(this.wd,o.getWd());
            }
            return c2;
        }
        return c1;
    }

    /**
     * @param out <code>DataOuput</code> to serialize this object into.
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(year);
        out.writeInt(month);
        out.writeInt(day);
        out.writeDouble(wd);
    }

    /**
     * @param in <code>DataInput</code> to deseriablize this object from.
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.year = in.readInt();
        this.month = in.readInt();
        this.day = in.readInt();
        this.wd = in.readDouble();
    }
}
