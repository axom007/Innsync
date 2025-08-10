import express from 'express';import { readFileSync } from 'fs';import { parse } from 'csv-parse/sync';import pg from 'pg';
const app=express();app.use(express.json());const pool=new pg.Pool({host:process.env.PGHOST||'localhost',user:process.env.PGUSER||'postgres',password:process.env.PGPASSWORD||'postgres',database:process.env.PGDATABASE||'innsync',port:Number(process.env.PGPORT||5432),});
async function query(sql,params){const c=await pool.connect();try{return await c.query(sql,params);}finally{c.release();}}
const schemaSQL=`
CREATE TABLE IF NOT EXISTS hotels (id uuid primary key, name text, timezone text, address text);
CREATE TABLE IF NOT EXISTS users (id uuid primary key, email text unique, password_demo text, status text);
CREATE TABLE IF NOT EXISTS hotel_users (hotel_id uuid, user_id uuid, role text, primary key (hotel_id, user_id));
CREATE TABLE IF NOT EXISTS rooms (id uuid primary key, hotel_id uuid, number text, floor int, type text, rate int);
CREATE TABLE IF NOT EXISTS devices (id uuid primary key, hotel_id uuid, device_uid text unique, model text, status text);
CREATE TABLE IF NOT EXISTS room_devices (room_id uuid, device_id uuid, bound_at timestamptz, active boolean, primary key (room_id, device_id));
CREATE TABLE IF NOT EXISTS bookings (id uuid primary key, hotel_id uuid, room_id uuid, channel text, guest_name text, checkin_ts timestamptz, checkout_ts timestamptz, status text, price numeric, currency text);
CREATE TABLE IF NOT EXISTS stays (id uuid primary key, hotel_id uuid, room_id uuid, booking_id uuid, source text, checkin_ts timestamptz, checkout_ts timestamptz);
CREATE TABLE IF NOT EXISTS occupancy_events (id serial primary key, hotel_id uuid, device_id uuid, room_id uuid, event_ts timestamptz, event_type text, payload_json text);
CREATE TABLE IF NOT EXISTS alerts (id uuid primary key, hotel_id uuid, room_id uuid, type text, details_json text, severity text, created_at timestamptz, resolved_at timestamptz);
CREATE TABLE IF NOT EXISTS payments (id uuid primary key, hotel_id uuid, booking_id uuid, amount numeric, method text, received_ts timestamptz, reference text);
`;
const csvFiles=['hotels','users','hotel_users','rooms','devices','room_devices','bookings','stays','occupancy_events','alerts','payments'];
function readCSV(name){const p=`/app/data/innsync_dummy_${name}.csv`;const str=readFileSync(p,'utf8');return parse(str,{columns:true,skip_empty_lines:true});}
async function seedIfEmpty(){await query(schemaSQL);const {rows}=await query('select count(*)::int as n from hotels');if(rows[0].n>0) return;
const data=Object.fromEntries(csvFiles.map(n=>[n,readCSV(n)]));
function escId(x){return x?`'${x.replace(/'/g,"''")}'`:'NULL'}function escTxt(x){return x===undefined||x===null||x===''?'NULL':`'${String(x).replace(/'/g,"''")}'`}function escNum(x){return (x===undefined||x===null||x==='')?'NULL':Number(x)}function escBool(x){if(x===true||x==='true'||x==='True')return'true';if(x===false||x==='false'||x==='False')return'false';return'false'}function escTs(x){return x?`'${x}'`:'NULL'}
for (const r of data.hotels) await query(`insert into hotels values (${escId(r.id)},${escTxt(r.name)},${escTxt(r.timezone)},${escTxt(r.address)})`);
for (const r of data.users) await query(`insert into users values (${escId(r.id)},${escTxt(r.email)},${escTxt(r.password_demo)},${escTxt(r.status)})`);
for (const r of data.hotel_users) await query(`insert into hotel_users values (${escId(r.hotel_id)},${escId(r.user_id)},${escTxt(r.role)})`);
for (const r of data.rooms) await query(`insert into rooms values (${escId(r.id)},${escId(r.hotel_id)},${escTxt(r.number)},${escNum(r.floor)},${escTxt(r.type)},${escNum(r.rate)})`);
for (const r of data.devices) await query(`insert into devices values (${escId(r.id)},${escId(r.hotel_id)},${escTxt(r.device_uid)},${escTxt(r.model)},${escTxt(r.status)})`);
for (const r of data.room_devices) await query(`insert into room_devices values (${escId(r.room_id)},${escId(r.device_id)},${escTs(r.bound_at)},${escBool(r.active)})`);
for (const r of data.bookings) await query(`insert into bookings values (${escId(r.id)},${escId(r.hotel_id)},${escId(r.room_id)},${escTxt(r.channel)},${escTxt(r.guest_name)},${escTs(r.checkin_ts)},${escTs(r.checkout_ts)},${escTxt(r.status)},${escNum(r.price)},${escTxt(r.currency)})`);
for (const r of data.stays) await query(`insert into stays values (${escId(r.id)},${escId(r.hotel_id)},${escId(r.room_id)},${escId(r.booking_id)},${escTxt(r.source)},${escTs(r.checkin_ts)},${escTs(r.checkout_ts)})`);
for (const r of data.occupancy_events) await query(`insert into occupancy_events (hotel_id,device_id,room_id,event_ts,event_type,payload_json) values (${escId(r.hotel_id)},${escId(r.device_id)},${escId(r.room_id)},${escTs(r.event_ts)},${escTxt(r.event_type)},${escTxt(r.payload_json)})`);
for (const r of data.alerts) await query(`insert into alerts values (${escId(r.id)},${escId(r.hotel_id)},${escId(r.room_id)},${escTxt(r.type)},${escTxt(r.details_json)},${escTxt(r.severity)},${escTs(r.created_at)},${escTs(r.resolved_at)})`);
for (const r of data.payments) await query(`insert into payments values (${escId(r.id)},${escId(r.hotel_id)},${escId(r.booking_id)},${escNum(r.amount)},${escTxt(r.method)},${escTs(r.received_ts)},${escTxt(r.reference)})`);
console.log('Seeded database from CSV');}
app.get('/health',(_,res)=>res.json({ok:true}));
app.get('/rooms',async(req,res)=>{const {rows}=await query('select * from rooms order by number::int asc');res.json(rows);});
app.get('/devices',async(req,res)=>{const {rows}=await query(`select r.number as room_number,d.device_uid,d.model,d.status from room_devices rd join rooms r on r.id=rd.room_id join devices d on d.id=rd.device_id where rd.active=true order by r.number::int asc`);res.json(rows);});
app.get('/alerts',async(req,res)=>{const {rows}=await query(`select * from alerts where resolved_at is null order by created_at desc`);res.json(rows);});
app.get('/metrics/summary',async(req,res)=>{const rooms=await query('select count(*)::int as n from rooms');const total=rooms.rows[0].n;const bookings=await query(`select status,price from bookings where (status='booked' or status='checked_in') and checkin_ts<=now() and checkout_ts>=now()`);
const booked=bookings.rows.filter(b=>b.status==='booked').length;const checked=bookings.rows.filter(b=>b.status==='checked_in').length;const revenue=bookings.rows.filter(b=>b.status==='checked_in'&&b.price).reduce((n,b)=>n+Number(b.price||0),0);
const flags=(await query(`select count(*)::int as n from alerts where resolved_at is null and type='OccupancyWithoutCheckin'`)).rows[0].n;const occRate=Math.round(((booked+checked)/Math.max(1,total))*100);
res.json({occupancyRate:occRate,revenueToday:revenue,booked,flags,roomsTotal:total});});
app.post('/bookings',async(req,res)=>{const {room_id,channel='Walk-in',guest_name='Walk-in Guest',nights=1,price=3200}=req.body||{};const checkin=new Date();const checkout=new Date(checkin.getTime()+nights*24*3600*1000);
const id=crypto.randomUUID?.()||'00000000-0000-0000-000000000000';await query(`insert into bookings (id,hotel_id,room_id,channel,guest_name,checkin_ts,checkout_ts,status,price,currency) select $1,r.hotel_id,$2,$3,$4,$5,$6,'checked_in',$7,'INR' from rooms r where r.id=$2`,
[id,room_id,channel,guest_name,checkin.toISOString(),checkout.toISOString(),price]);res.json({ok:true,id});});
app.post('/ingest/occupancy',async(req,res)=>{const {device_uid,ts,type}=req.body||{};const dev=await query('select * from devices where device_uid=$1',[device_uid]);if(!dev.rows.length)return res.status(404).json({error:'Unknown device'});
const bind=await query('select * from room_devices where device_id=$1 and active=true limit 1',[dev.rows[0].id]);if(!bind.rows.length)return res.status(404).json({error:'Unbound device'});const roomId=bind.rows[0].room_id;
await query(`insert into occupancy_events (hotel_id,device_id,room_id,event_ts,event_type,payload_json) values ($1,$2,$3,$4,$5,$6)`,[dev.rows[0].hotel_id,dev.rows[0].id,roomId,ts||new Date().toISOString(),type||'presence','{}']);
const active=await query(`select * from bookings where room_id=$1 and (status='booked' or status='checked_in') and checkin_ts<=now() and checkout_ts>=now() limit 1`,[roomId]);
if(active.rows.length){await query('update bookings set status=$1 where id=$2',['checked_in',active.rows[0].id]);}else{await query(`insert into alerts (id,hotel_id,room_id,type,details_json,severity,created_at) values ($1,$2,$3,'OccupancyWithoutCheckin',$4,'medium',now())`,
[crypto.randomUUID?.()||'00000000-0000-0000-000000000000',dev.rows[0].hotel_id,roomId,'"Presence detected without check-in"']);}
res.json({ok:true});});
const PORT=3000;(async()=>{await seedIfEmpty();app.listen(PORT,()=>console.log('API on http://localhost:'+PORT));})();
