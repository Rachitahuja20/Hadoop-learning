package com.aamend.hadoop.MapReduce;

import java.io.DataInput;

import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.log4j.Logger;




public class ClickKey implements WritableComparable<ClickKey> {


  private Logger logger = Logger.getLogger("ClickKey");
  private String countryName;

  private Double income;



  public void write(DataOutput out) throws IOException {

    out.writeUTF(getCountryName());

    out.writeDouble(getIncome());

  }



  public void readFields(DataInput in) throws IOException {

    this.setCountryName(in.readUTF());

    this.setIncome(in.readDouble());

  }



  public int compareTo(ClickKey other) {

    int res = this.getIncome().compareTo(other.getIncome());

    return res;

  }



  @Override
  public int hashCode() {

    int hash = 7;


    hash = 13 * hash + (this.getIncome() != null ? this.getIncome().hashCode() : 0);
    logger.info("Stage - Hash");
    return hash;

  }


  @Override
  public boolean equals(Object obj) {

    final ClickKey other = (ClickKey) obj;
    logger.info("Stage - equals");
    return this.getCountryName().equals(other.getCountryName())
        && this.getIncome().equals(other.getIncome());

  }

  public int compareGroup(ClickKey other) {

    int res = this.getIncome().compareTo(other.getIncome());
    logger.info("Stage - comparegroup");
    return res;

}

  public String getCountryName() {
    return countryName;
  }

  public void setCountryName(String countryName) {
    this.countryName = countryName;
  }

  public Double getIncome() {
    return income;
  }

  public void setIncome(double income) {
    this.income = income;
  }

  public static class GroupComparator extends WritableComparator {

    public GroupComparator() {

      super(ClickKey.class, true);

    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

      ClickKey w1 = (ClickKey) a;

      ClickKey w2 = (ClickKey) b;

      return w1.compareGroup(w2);

    }

  }

}

