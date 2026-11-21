//! Minimal, zero-dependency, high-speed Uncompressed PLAIN Parquet writer
//! tailored specifically for `user_daily` and `campaign_daily`.

use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::Path;

// Thrift Compact Protocol type IDs
const CT_I32: u8 = 5;
const CT_I64: u8 = 6;
const CT_BINARY: u8 = 8;
const CT_LIST: u8 = 9;
const CT_STRUCT: u8 = 12;

// Parquet Physical Types
const TYPE_INT32: i32 = 1;
const TYPE_INT64: i32 = 2;
const TYPE_BYTE_ARRAY: i32 = 6;

// Parquet FieldRepetitionType
const REP_REQUIRED: i32 = 0;
const REP_OPTIONAL: i32 = 1;

// Parquet ConvertedType
const CONV_UTF8: i32 = 0;
const CONV_DATE: i32 = 6;

#[inline(always)]
fn push_uvarint(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}

#[inline(always)]
fn push_i32(buf: &mut Vec<u8>, prev_field: &mut i16, field_id: i16, val: i32) {
    push_field_header(buf, prev_field, field_id, CT_I32);
    let zz = ((val << 1) ^ (val >> 31)) as u32;
    push_uvarint(buf, zz as u64);
}

#[inline(always)]
fn push_i64(buf: &mut Vec<u8>, prev_field: &mut i16, field_id: i16, val: i64) {
    push_field_header(buf, prev_field, field_id, CT_I64);
    let zz = ((val << 1) ^ (val >> 63)) as u64;
    push_uvarint(buf, zz);
}

#[inline(always)]
fn push_binary(buf: &mut Vec<u8>, prev_field: &mut i16, field_id: i16, bytes: &[u8]) {
    push_field_header(buf, prev_field, field_id, CT_BINARY);
    push_uvarint(buf, bytes.len() as u64);
    buf.extend_from_slice(bytes);
}

#[inline(always)]
fn push_field_header(buf: &mut Vec<u8>, prev_field: &mut i16, field_id: i16, ctype: u8) {
    let delta = field_id - *prev_field;
    if (1..=15).contains(&delta) {
        buf.push(((delta as u8) << 4) | ctype);
    } else {
        buf.push(ctype);
        let zz = ((field_id << 1) ^ (field_id >> 15)) as u16;
        push_uvarint(buf, zz as u64);
    }
    *prev_field = field_id;
}

#[inline(always)]
fn push_list_header(buf: &mut Vec<u8>, prev_field: &mut i16, field_id: i16, elem_type: u8, len: usize) {
    push_field_header(buf, prev_field, field_id, CT_LIST);
    if len < 15 {
        buf.push(((len as u8) << 4) | elem_type);
    } else {
        buf.push(0xF0 | elem_type);
        push_uvarint(buf, len as u64);
    }
}

struct ColSpec<'a> {
    name: &'a str,
    phys_type: i32,
    rep_type: i32,
    conv_type: Option<i32>,
}

struct WrittenCol {
    data_page_offset: i64,
    total_size: i64,
}

fn encode_data_page_header(num_values: i32, page_bytes_len: i32) -> Vec<u8> {
    let mut hdr = Vec::with_capacity(32);
    let mut pf = 0i16;
    push_i32(&mut hdr, &mut pf, 1, 0); // PageType::DATA_PAGE = 0
    push_i32(&mut hdr, &mut pf, 2, page_bytes_len);
    push_i32(&mut hdr, &mut pf, 3, page_bytes_len);
    push_field_header(&mut hdr, &mut pf, 5, CT_STRUCT); // data_page_header
    {
        let mut dpf = 0i16;
        push_i32(&mut hdr, &mut dpf, 1, num_values);
        push_i32(&mut hdr, &mut dpf, 2, 0); // Encoding::PLAIN = 0
        push_i32(&mut hdr, &mut dpf, 3, 3); // Encoding::RLE = 3
        push_i32(&mut hdr, &mut dpf, 4, 3); // Encoding::RLE = 3
        hdr.push(0); // end DataPageHeader
    }
    hdr.push(0); // end PageHeader
    hdr
}

fn write_column_page(
    writer: &mut impl Write,
    offset: &mut i64,
    num_rows: usize,
    page_payload: &[u8],
) -> io::Result<WrittenCol> {
    let page_offset = *offset;
    let hdr = encode_data_page_header(num_rows as i32, page_payload.len() as i32);
    writer.write_all(&hdr)?;
    writer.write_all(page_payload)?;
    let total_size = (hdr.len() + page_payload.len()) as i64;
    *offset += total_size;
    Ok(WrittenCol {
        data_page_offset: page_offset,
        total_size,
    })
}

fn write_parquet_footer(
    writer: &mut impl Write,
    cols: &[ColSpec],
    written_cols: &[WrittenCol],
    num_rows: usize,
) -> io::Result<()> {
    let mut footer = Vec::with_capacity(512);
    let mut pf = 0i16;
    push_i32(&mut footer, &mut pf, 1, 1); // version = 1

    // schema: 1 root + cols.len() leaves
    push_list_header(&mut footer, &mut pf, 2, CT_STRUCT, 1 + cols.len());
    {
        // root schema element
        let mut spf = 0i16;
        push_binary(&mut footer, &mut spf, 4, b"schema");
        push_i32(&mut footer, &mut spf, 5, cols.len() as i32);
        footer.push(0);
    }
    for c in cols {
        let mut spf = 0i16;
        push_i32(&mut footer, &mut spf, 1, c.phys_type);
        push_i32(&mut footer, &mut spf, 3, c.rep_type);
        push_binary(&mut footer, &mut spf, 4, c.name.as_bytes());
        if let Some(ct) = c.conv_type {
            push_i32(&mut footer, &mut spf, 6, ct);
        }
        footer.push(0);
    }

    push_i64(&mut footer, &mut pf, 3, num_rows as i64);

    if num_rows == 0 {
        push_list_header(&mut footer, &mut pf, 4, CT_STRUCT, 0);
    } else {
        push_list_header(&mut footer, &mut pf, 4, CT_STRUCT, 1);
        let mut rg_pf = 0i16;
        push_list_header(&mut footer, &mut rg_pf, 1, CT_STRUCT, cols.len());
        let mut total_byte_size = 0i64;
        for (c, wc) in cols.iter().zip(written_cols.iter()) {
            total_byte_size += wc.total_size;
            let mut cc_pf = 0i16;
            push_i64(&mut footer, &mut cc_pf, 2, wc.data_page_offset);
            push_field_header(&mut footer, &mut cc_pf, 3, CT_STRUCT); // meta_data
            {
                let mut md_pf = 0i16;
                push_i32(&mut footer, &mut md_pf, 1, c.phys_type);
                push_list_header(&mut footer, &mut md_pf, 2, CT_I32, 2);
                // list<i32> elements are zigzag varints directly
                push_uvarint(&mut footer, 0); // PLAIN = 0
                push_uvarint(&mut footer, ((3i32 << 1) ^ (3i32 >> 31)) as u64); // RLE = 3
                push_list_header(&mut footer, &mut md_pf, 3, CT_BINARY, 1);
                push_uvarint(&mut footer, c.name.len() as u64);
                footer.extend_from_slice(c.name.as_bytes());
                push_i32(&mut footer, &mut md_pf, 4, 0); // UNCOMPRESSED = 0
                push_i64(&mut footer, &mut md_pf, 5, num_rows as i64);
                push_i64(&mut footer, &mut md_pf, 6, wc.total_size);
                push_i64(&mut footer, &mut md_pf, 7, wc.total_size);
                push_i64(&mut footer, &mut md_pf, 9, wc.data_page_offset);
                footer.push(0); // end ColumnMetaData
            }
            footer.push(0); // end ColumnChunk
        }
        push_i64(&mut footer, &mut rg_pf, 2, total_byte_size);
        push_i64(&mut footer, &mut rg_pf, 3, num_rows as i64);
        footer.push(0); // end RowGroup
    }

    footer.push(0); // end FileMetaData

    let footer_len = footer.len() as u32;
    writer.write_all(&footer)?;
    writer.write_all(&footer_len.to_le_bytes())?;
    writer.write_all(b"PAR1")?;
    Ok(())
}

#[derive(Clone, Copy)]
pub struct UserDailyRow {
    pub person_id: u64,
    pub day: i32,
    pub sessions: u32,
    pub events: u32,
    pub revenue_cents: i64,
    pub top_cat: u8, // 0xFF = NULL
}

#[inline(always)]
pub fn unpack7(val: u64, buf: &mut [u8; 9]) -> usize {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        let hi8 = unsafe { core::arch::x86_64::_pdep_u64(val >> 7, 0x7F7F_7F7F_7F7F_7F7F) };
        let bytes8 = hi8.to_be_bytes();
        unsafe {
            std::ptr::copy_nonoverlapping(bytes8.as_ptr(), buf.as_mut_ptr(), 8);
        }
        let b8 = (val & 0x7F) as u8;
        buf[8] = b8;
        if b8 != 0 {
            return 9;
        }
        // Find first zero byte in bytes8
        let mut len = 0;
        while len < 8 && buf[len] != 0 {
            len += 1;
        }
        return len;
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        let mut len = 0;
        let mut shift = 56u32;
        while len < 9 {
            let b = ((val >> shift) & 0x7F) as u8;
            if b == 0 {
                break;
            }
            buf[len] = b;
            len += 1;
            shift = shift.wrapping_sub(7);
        }
        len
    }
}

pub fn write_user_daily_parquet(
    path: &Path,
    rows: &[UserDailyRow],
    categories: &[Vec<u8>],
) -> io::Result<()> {
    let mut writer = BufWriter::with_capacity(1 << 20, File::create(path)?);
    writer.write_all(b"PAR1")?;
    let mut offset = 4i64;
    let n = rows.len();

    let cols = [
        ColSpec { name: "user_id", phys_type: TYPE_BYTE_ARRAY, rep_type: REP_REQUIRED, conv_type: Some(CONV_UTF8) },
        ColSpec { name: "day", phys_type: TYPE_INT32, rep_type: REP_REQUIRED, conv_type: Some(CONV_DATE) },
        ColSpec { name: "sessions", phys_type: TYPE_INT64, rep_type: REP_REQUIRED, conv_type: None },
        ColSpec { name: "events", phys_type: TYPE_INT64, rep_type: REP_REQUIRED, conv_type: None },
        ColSpec { name: "revenue_cents", phys_type: TYPE_INT64, rep_type: REP_REQUIRED, conv_type: None },
        ColSpec { name: "top_category", phys_type: TYPE_BYTE_ARRAY, rep_type: REP_OPTIONAL, conv_type: Some(CONV_UTF8) },
    ];

    let mut written_cols = Vec::with_capacity(6);
    if n > 0 {
        let mut buf = Vec::with_capacity(n * 13);

        // 1. user_id
        let mut ubuf = [0u8; 9];
        for r in rows {
            let ulen = unpack7(r.person_id, &mut ubuf);
            buf.extend_from_slice(&(ulen as u32).to_le_bytes());
            buf.extend_from_slice(&ubuf[..ulen]);
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 2. day (INT32)
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&r.day.to_le_bytes());
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 3. sessions (INT64)
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&(r.sessions as i64).to_le_bytes());
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 4. events (INT64)
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&(r.events as i64).to_le_bytes());
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 5. revenue_cents (INT64)
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&r.revenue_cents.to_le_bytes());
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 6. top_category (OPTIONAL BYTE_ARRAY)
        buf.clear();
        let num_groups = (n + 7) / 8;
        let mut rle_header = Vec::with_capacity(8);
        push_uvarint(&mut rle_header, ((num_groups as u64) << 1) | 1);
        let def_byte_len = (rle_header.len() + num_groups) as u32;
        buf.extend_from_slice(&def_byte_len.to_le_bytes());
        buf.extend_from_slice(&rle_header);
        let bit_start = buf.len();
        buf.resize(bit_start + num_groups, 0u8);
        for (i, r) in rows.iter().enumerate() {
            if r.top_cat != 0xFF {
                buf[bit_start + (i >> 3)] |= 1u8 << (i & 7);
            }
        }
        for r in rows {
            if r.top_cat != 0xFF {
                let cat_bytes = &categories[r.top_cat as usize];
                buf.extend_from_slice(&(cat_bytes.len() as u32).to_le_bytes());
                buf.extend_from_slice(cat_bytes);
            }
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);
    }

    write_parquet_footer(&mut writer, &cols, &written_cols, n)?;
    writer.flush()
}

pub struct CampaignDailyRow<'a> {
    pub campaign_id: &'a [u8],
    pub channel: &'a [u8],
    pub day: i32,
    pub sessions: i64,
    pub purchasing_sessions: i64,
    pub revenue_cents: i64,
    pub assisted_sessions: i64,
}

pub fn write_campaign_daily_parquet(
    path: &Path,
    rows: &[CampaignDailyRow],
) -> io::Result<()> {
    let mut writer = BufWriter::with_capacity(256 * 1024, File::create(path)?);
    writer.write_all(b"PAR1")?;
    let mut offset = 4i64;
    let n = rows.len();

    let cols = [
        ColSpec { name: "campaign_id", phys_type: TYPE_BYTE_ARRAY, rep_type: REP_REQUIRED, conv_type: Some(CONV_UTF8) },
        ColSpec { name: "channel", phys_type: TYPE_BYTE_ARRAY, rep_type: REP_REQUIRED, conv_type: Some(CONV_UTF8) },
        ColSpec { name: "day", phys_type: TYPE_INT32, rep_type: REP_REQUIRED, conv_type: Some(CONV_DATE) },
        ColSpec { name: "sessions", phys_type: TYPE_INT64, rep_type: REP_REQUIRED, conv_type: None },
        ColSpec { name: "purchasing_sessions", phys_type: TYPE_INT64, rep_type: REP_REQUIRED, conv_type: None },
        ColSpec { name: "revenue_cents", phys_type: TYPE_INT64, rep_type: REP_REQUIRED, conv_type: None },
        ColSpec { name: "assisted_sessions", phys_type: TYPE_INT64, rep_type: REP_REQUIRED, conv_type: None },
    ];

    let mut written_cols = Vec::with_capacity(7);
    if n > 0 {
        let mut buf = Vec::with_capacity(n * 12);

        // 1. campaign_id
        for r in rows {
            buf.extend_from_slice(&(r.campaign_id.len() as u32).to_le_bytes());
            buf.extend_from_slice(r.campaign_id);
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 2. channel
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&(r.channel.len() as u32).to_le_bytes());
            buf.extend_from_slice(r.channel);
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 3. day
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&r.day.to_le_bytes());
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 4. sessions
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&r.sessions.to_le_bytes());
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 5. purchasing_sessions
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&r.purchasing_sessions.to_le_bytes());
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 6. revenue_cents
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&r.revenue_cents.to_le_bytes());
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);

        // 7. assisted_sessions
        buf.clear();
        for r in rows {
            buf.extend_from_slice(&r.assisted_sessions.to_le_bytes());
        }
        written_cols.push(write_column_page(&mut writer, &mut offset, n, &buf)?);
    }

    write_parquet_footer(&mut writer, &cols, &written_cols, n)?;
    writer.flush()
}
