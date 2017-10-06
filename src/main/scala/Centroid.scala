package com.schwartzmx.tdigest


class Centroid (var mean: Double, var count: Double) extends Ordered[Centroid]{

    def update(x: Double, weight: Double) : Unit = {
        this.count = this.count + weight
        this.mean += weight * (x - this.mean) / this.count
    }

    def instanceOf(a: Any) = a.isInstanceOf[Centroid]
    override def equals(that: Any) : Boolean = {
        that match {
            case that: Centroid => that.instanceOf(this) && this.mean == that.mean && this.count == that.count
            case _ => false
        }
    }

    def compare(that: Centroid) = (this.mean - that.mean).toInt

    override def toString() : String = s"<Centroid: mean=${this.mean}, count=${this.count}>"
}