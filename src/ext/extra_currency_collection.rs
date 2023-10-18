use everscale_types::models::ExtraCurrencyCollection;
use everscale_types::num::VarUint248;
use everscale_vm::{error, types::Result};

pub trait ExtraCurrencyCollectionExt {
    /// merge two dictionaries by adding its contents
    fn try_add_assign(&mut self, rhs: &ExtraCurrencyCollection) -> Result<()>;
    ///FIXME very strange operation, check every place it occurs
    /// subtract every rhs token from self, ignore non-existing token and underflow
    fn try_sub_assign(&mut self, rhs: &ExtraCurrencyCollection) -> Result<bool>;
    fn is_zero(&self) -> Result<bool>;
}

impl ExtraCurrencyCollectionExt for ExtraCurrencyCollection {
    fn try_add_assign(&mut self, rhs: &ExtraCurrencyCollection) -> Result<()> {
        let dict = self.as_dict_mut();
        for entry in rhs.as_dict().iter() {
            let (key, value) = entry?;
            match dict.get(key)? {
                Some(old) => dict.set(key, checked_add(old, value)
                    .ok_or_else(|| error!("integer overflow"))?)?,
                None => dict.add(key, value)?,
            };
        }
        Ok(())
    }

    fn try_sub_assign(&mut self, rhs: &ExtraCurrencyCollection) -> Result<bool> {
        let dict = self.as_dict_mut();
        for entry in rhs.as_dict().iter() {
            let (key, value) = entry?;
            match dict.get(key)? {
                Some(old) if old >= value => {
                    // it was ok for old currency collection impl to stay with zero values
                    dict.set(key, checked_sub(old, value).ok_or_else(|| error!("infallible subtract of extra currencies"))?)?;
                }
                _ => return Ok(false)
            };
        }
        Ok(true)
    }

    fn is_zero(&self) -> Result<bool> {
        if self.is_empty() {
            return Ok(true);
        }
        for value in self.as_dict().values() {
            if value?.is_zero() == false {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

fn checked_add(a: VarUint248, b: VarUint248) -> Option<VarUint248> {
    let (a_high, a_low) = a.into_words();
    let (b_high, b_low) = b.into_words();
    let (low_overflow, low) = if a_low > u128::MAX - b_low {
        (1, a_low - (u128::MAX - b_low) - 1)
    } else {
        (0, a_low + b_low)
    };
    const HIGH_MAX: u128 = VarUint248::MAX.into_words().0;
    if a_high > HIGH_MAX || b_high > HIGH_MAX || a_high + low_overflow > HIGH_MAX - b_high {
        None
    } else {
        Some(VarUint248::from_words(a_high + low_overflow + b_high, low))
    }
}

fn checked_sub(a: VarUint248, b: VarUint248) -> Option<VarUint248> {
    let (a_high, a_low) = a.into_words();
    let (b_high, b_low) = b.into_words();
    // let high = a_high - b_high;
    let (low_underflow, low) = if a_low < b_low {
        (1, a_low + (u128::MAX - b_low) + 1)
    } else {
        (0, a_low - b_low)
    };
    const HIGH_MAX: u128 = VarUint248::MAX.into_words().0;
    if a_high > HIGH_MAX || b_high > HIGH_MAX || a_high < low_underflow + b_high {
        None
    } else {
        Some(VarUint248::from_words(a_high - low_underflow - b_high, low))
    }
}

#[test]
fn add_test() {
    const HIGH_MAX: u128 = VarUint248::MAX.into_words().0;
    // max overflow
    assert_eq!(
        Some(VarUint248::MAX),
        checked_add(
            VarUint248::from_words(HIGH_MAX, u128::MAX - 1),
            VarUint248::new(1)
        )
    );
    assert_eq!(
        Some(VarUint248::MAX),
        checked_add(
            VarUint248::from_words(HIGH_MAX, u128::MAX),
            VarUint248::new(0)
        )
    );
    assert_eq!(
        None,
        checked_add(
            VarUint248::from_words(HIGH_MAX, u128::MAX),
            VarUint248::new(1)
        )
    );
    // low part overflow
    assert_eq!(
        Some(VarUint248::from_words(0, u128::MAX)),
        checked_add(VarUint248::new(u128::MAX - 1), VarUint248::new(1))
    );
    assert_eq!(
        Some(VarUint248::from_words(0, u128::MAX)),
        checked_add(VarUint248::new(u128::MAX), VarUint248::new(0))
    );
    assert_eq!(
        Some(VarUint248::from_words(1, 0)),
        checked_add(VarUint248::new(u128::MAX), VarUint248::new(1))
    );
    assert_eq!(
        Some(VarUint248::from_words(1, 1)),
        checked_add(VarUint248::new(u128::MAX), VarUint248::new(2))
    );
    // basic
    assert_eq!(
        Some(VarUint248::from_words(0, u128::MAX - 1)),
        checked_add(VarUint248::new(u128::MAX - 2), VarUint248::new(1))
    );
    assert_eq!(
        Some(VarUint248::from_words(0, 1)),
        checked_add(VarUint248::new(0), VarUint248::new(1))
    );
}

#[test]
fn sub_test() {
    const HIGH_MAX: u128 = VarUint248::MAX.into_words().0;
    assert_eq!(
        Some(VarUint248::new(u128::MAX)),
        checked_sub(VarUint248::from_words(1, 1), VarUint248::new(2))
    );
    assert_eq!(
        Some(VarUint248::new(u128::MAX)),
        checked_sub(VarUint248::from_words(1, 0), VarUint248::new(1))
    );
    assert_eq!(
        Some(VarUint248::new(u128::MAX)),
        checked_sub(VarUint248::from_words(1, 1), VarUint248::new(2))
    );
    assert_eq!(
        Some(VarUint248::new(u128::MAX)),
        checked_sub(VarUint248::MAX, VarUint248::from_words(HIGH_MAX, 0))
    );
    assert_eq!(
        Some(VarUint248::from_words(HIGH_MAX, 0)),
        checked_sub(VarUint248::MAX, VarUint248::new(u128::MAX))
    );
    assert_eq!(
        Some(VarUint248::from_words(1, 1)),
        checked_sub(VarUint248::from_words(3, 3), VarUint248::from_words(2, 2))
    );
}
