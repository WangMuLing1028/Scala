package cn.sibat.bus

import java.sql.Timestamp
import java.util.Date

/**
  * 站点数据
  * 线路，站点名称，来回站点标识（上行01,下行02），站点Id，站点名称，站点序号，站点经度，站点纬度
  * Created by kong on 2017/4/11.
  *
  * @param route         线路
  * @param lineName      线路名称
  * @param direct        方向
  * @param stationId     站点ID
  * @param stationName   站点名称
  * @param stationSeqId  站点序号
  * @param stationLon    站点经度
  * @param stationLat    站点纬度
  * @param checkpointInd 站点最近的checkpoint点
  */
case class StationData(route: String, lineName: String, direct: String, stationId: String, stationName: String, stationSeqId: Int, stationLon: Double, stationLat: Double, checkpointInd: Int)

/**
  * 公交刷卡数据
  *
  * @param rId         记录编码
  * @param lId         卡片逻辑编码
  * @param term        终端编码
  * @param tradeType   交易类型
  * @param time        拍卡时间
  * @param companyName 公司名称
  * @param route       线路名称
  * @param carId       车牌号
  */
case class BusCardData(rId: String, lId: String, term: String, tradeType: String, time: String, companyName: String, route: String, carId: String)

case class Trip(carId: String, route: String, direct: String, firstSeqIndex: Int, ld: Double, nextSeqIndex: Int, rd: Double, tripId: Int) {
  override def toString: String = route + "," + direct + "," + firstSeqIndex + "," + ld + "," + nextSeqIndex + "," + rd
}

/**
  * 趟次可视化实体类
  *
  * @param index           序号
  * @param tripId          趟次
  * @param frechetDistance 弗雷歇距离
  */
case class TripVisualization(index: Int, tripId: Int, frechetDistance: Double)

/**
  * 公交到站可视化实体
  *
  * @param carId  车牌号
  * @param lon    经度
  * @param lat    纬度
  * @param route  线路
  * @param direct 方向
  * @param upTime 上传时间
  * @param tripId 班次号
  */
case class BusArrivalForVisual(carId: String, lon: Double, lat: Double, route: String, direct: String, upTime: String, tripId: Int)

/**
  * 公交到站HBase存储数据格式
  * 表名 ARRLEA_Q_+日期
  * family => arrlea
  * typeCode => (01,公交车),(02,出租车),(03,货运车),(04,城际班车),(98,其他车辆),(99,未知车辆)
  * cityCode => (01,深圳市),(02,广州市),(03,惠州市),(04,珠海市)(05,中山市),(06,广东省),(98,全国),(99,不区分城市)
  *
  * @param rowKey          typeCode|carId|tripId|stationIndex
  * @param tripId          32位UUID
  * @param lineId          线路ID
  * @param direct          方向 up,down
  * @param stationIndex    站点站序
  * @param stationId       站点ID
  * @param arrivalTime     到站时间
  * @param leaveTime       离站时间
  * @param prefixStationId 上一站点ID
  * @param nextStationId   下一站点ID
  */
case class BusArrivalHBase(rowKey: String, tripId: String, lineId: String, direct: String, stationIndex: String, stationId: String, arrivalTime: String, leaveTime: String, prefixStationId: String, nextStationId: String)

/**
  * 公交到站HBase存储数据格式
  * 表名 ARRLEA_Q_+日期
  * family => arrlea
  * typeCode => (01,公交车),(02,出租车),(03,货运车),(04,城际班车),(98,其他车辆),(99,未知车辆)
  * cityCode => (01,深圳市),(02,广州市),(03,惠州市),(04,珠海市)(05,中山市),(06,广东省),(98,全国),(99,不区分城市)
  *
  * @param rowKey          typeCode|carId|tripId|stationIndex
  * @param tripId          32位UUID
  * @param lineId          线路ID
  * @param direct          方向 up,down
  * @param stationIndex    站点站序
  * @param stationId       站点ID
  * @param arrivalTime     到站时间
  * @param leaveTime       离站时间
  * @param prefixStationId 上一站点ID
  * @param nextStationId   下一站点ID
  * @param trip_mile       里程
  * @param total           总站点
  */
case class BusArrivalHBase2(rowKey: String, tripId: String, lineId: String, direct: String, stationIndex: Int, stationId: String, arrivalTime: String, leaveTime: String, prefixStationId: String, nextStationId: String, trip_mile: Double, total: Int)

/**
  * 3.200的MySQL bus_roundtrip
  *
  * @param id                  id(uuid)
  * @param create_date         创建时间(yyyy-MM-dd HH:mm:ss)
  * @param modify_date         修改时间(yyyy-MM-dd HH:mm:ss)
  * @param bus_id              车牌号
  * @param line_dir            方向(up,down)
  * @param end_station_id      终点站ID
  * @param end_station_index   终点站索引
  * @param end_gps_time        结束时间(yyyy-MM-dd HH:mm:ss)
  * @param est_end_time        推测结束时间(yyyy-MM-dd HH:mm:ss)
  * @param est_start_time      推测开始时间(yyyy-MM-dd HH:mm:ss)
  * @param line_id             线路编号(lineId)
  * @param miss_station_list   缺失站点列表(index1/index2...)
  * @param real_ratio          实际到站率(realTotal/total)
  * @param service_date        日期
  * @param start_station_id    起始站ID
  * @param start_station_index 起始站索引
  * @param start_gps_time      开始时间(yyyy-MM-dd HH:mm:ss)
  * @param trip_timecost       耗时
  * @param total_ratio         总到站率
  * @param trip_mile           里程
  * @param line_dir_id         线路ID uuid
  */
case class BusArrivalMySQL200(id: String, create_date: Timestamp, modify_date: Timestamp, bus_id: String, line_dir: String
                              , end_station_id: String, end_station_index: Int, end_gps_time: Timestamp, est_end_time: Timestamp
                              , est_start_time: Timestamp, line_id: String, miss_station_list: String, real_ratio: String
                              , service_date: Timestamp, start_station_id: String, start_station_index: Int, start_gps_time: Timestamp
                              , trip_timecost: Int, total_ratio: String, trip_mile: Double, line_dir_id: String)

/**
  * 3.205的MySQL roundtrip
  *
  * @param id                id
  * @param day               日期(yyyy-MM-dd)
  * @param linename          线路名称(name not lineId)
  * @param bus               车牌号
  * @param direction         方向(上行、下行)
  * @param startstation      起始站点名称
  * @param endstation        终点站点名称
  * @param starttime         发车时间(yyyy-MM-dd HH:mm:ss)
  * @param endtime           到达时间(yyyy-MM-dd HH:mm:ss)
  * @param dummystarttime    推测的发车时间(yyyy-MM-dd HH:mm:ss)
  * @param dummyendtime      推测的到达时间(yyyy-MM-dd HH:mm:ss)
  * @param dummystartstation 推测起始站点名称
  * @param dummyendstation   推测终点站点名称
  * @param missstationlist   缺失站点序列(index1/index2...)
  * @param realratio            实际到站率(realTotal/total)
  * @param totalratio            总到站率(realTotal/total)
  * @param timecost      耗时
  */
case class BusArrivalMySQL205(id: String, day: String, linename: String, bus: String, direction: String
                              , startstation: String, endstation: String, starttime: String, endtime: String
                              , dummystarttime: String, dummyendtime: String, dummystartstation: String, dummyendstation: String
                              , missstationlist: String, realratio: String, totalratio: String, timecost: String)

/**
  * 线路的checkpoint信息
  *
  * @param lon    经度
  * @param lat    纬度
  * @param lineId 线路ID
  * @param order  位置
  * @param direct 方向
  */
case class LineCheckPoint(lon: Double, lat: Double, lineId: String, order: Int, direct: String)

/**
  * 定位gps所在checkpoint的位置
  *
  * @param index      数据的索引位置
  * @param lon        经度
  * @param lat        纬度
  * @param gpsTime    gps时间
  * @param indexLeft  checkpoint的左位置
  * @param disL       距离checkpoint左位置的距离
  * @param indexRight checkpoint的右位置
  * @param disR       距离checkpoint右位置的距离
  * @param costTime   与下个gps点的时间差
  * @param speed      速度
  */
case class BusLocation(index: Int, lon: Double, lat: Double, gpsTime: String, indexLeft: Int, disL: Double, indexRight: Int, disR: Double, costTime: Long, speed: Double)

/**
  * Created by kong on 2017/6/22.
  */
case class CaseClassSet()
